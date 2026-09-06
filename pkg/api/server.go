package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"gopkg.in/yaml.v3"

	"github.com/Lab-OpenFlow/openflow/pkg/connectors"
	"github.com/Lab-OpenFlow/openflow/pkg/engine"
	"github.com/Lab-OpenFlow/openflow/pkg/model"
	"github.com/Lab-OpenFlow/openflow/pkg/security"
	"github.com/Lab-OpenFlow/openflow/pkg/storage"
)

// ServerConfig configures the API server.
type ServerConfig struct {
	Port        string
	StaticFS    fs.FS // Optional embedded frontend assets
	EnableAuth  bool
	AuthManager *security.AuthManager
	NodeID      string
	HashRing    *engine.HashRing
}

// Server provides REST, WebSocket, and Web UI endpoints for OpenFlow.
type Server struct {
	config   ServerConfig
	engine   *engine.Engine
	store    storage.Store
	registry *connectors.Registry
	auth     *security.AuthManager
	wsHub    *WSHub
	router   *gin.Engine
	hashRing *engine.HashRing
}

// NewServer initializes the OpenFlow API and HTTP server.
func NewServer(cfg ServerConfig, eng *engine.Engine, store storage.Store, reg *connectors.Registry) *Server {
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.Use(gin.Recovery())

	// CORS Middleware
	router.Use(func(c *gin.Context) {
		c.Writer.Header().Set("Access-Control-Allow-Origin", "*")
		c.Writer.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS, PATCH")
		c.Writer.Header().Set("Access-Control-Allow-Headers", "Origin, Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization, X-API-Key, X-Tenant-ID, X-Namespace, X-Idempotency-Key")
		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(http.StatusNoContent)
			return
		}
		c.Next()
	})

	// Tenant Extraction Middleware (Multi-Tenancy / Namespaces)
	router.Use(func(c *gin.Context) {
		tenantID := c.GetHeader("X-Tenant-ID")
		if tenantID == "" {
			tenantID = c.GetHeader("X-Namespace")
		}
		if tenantID == "" {
			tenantID = security.DefaultTenant
		}
		ctx := security.WithTenant(c.Request.Context(), tenantID)
		c.Request = c.Request.WithContext(ctx)
		c.Set("tenant_id", tenantID)
		c.Next()
	})

	wsHub := NewWSHub()
	go wsHub.Run()

	// Connect engine event emitter to WebSocket Hub
	eng.SubscribeEventListener(func(event model.WorkflowEvent) {
		wsHub.Broadcast(event)
	})

	authMgr := cfg.AuthManager
	if authMgr == nil {
		authMgr = security.NewAuthManager("")
	}

	srv := &Server{
		config:   cfg,
		engine:   eng,
		store:    store,
		registry: reg,
		auth:     authMgr,
		wsHub:    wsHub,
		router:   router,
		hashRing: cfg.HashRing,
	}

	srv.setupRoutes()
	return srv
}

// SetHashRing sets the consistent hash ring for distributed execution sharding.
func (s *Server) SetHashRing(ring *engine.HashRing) {
	s.hashRing = ring
}

func (s *Server) setupRoutes() {
	// 1. Health and Metrics
	healthHandler := func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"status":    "healthy",
			"version":   "3.0.0",
			"timestamp": time.Now(),
		})
	}
	s.router.GET("/health", healthHandler)
	s.router.GET("/healthz", healthHandler)
	s.router.GET("/metrics", gin.WrapH(promhttp.Handler()))

	// 2. Real-time WebSockets
	s.router.GET("/api/v1/ws", func(c *gin.Context) {
		s.wsHub.HandleWS(c.Writer, c.Request, "")
	})
	s.router.GET("/api/v1/ws/executions/:id", func(c *gin.Context) {
		id := c.Param("id")
		s.wsHub.HandleWS(c.Writer, c.Request, id)
	})

	// Rate Limiting: 50 requests/sec with burst capacity of 100 tokens per client
	execLimiter := RateLimitMiddleware(50, 100)

	// 3. Webhook Trigger Ingress
	s.router.POST("/api/v1/triggers/webhook/*path", execLimiter, s.handleWebhookTrigger)

	api := s.router.Group("/api/v1")
	{
		// Authentication & RBAC
		api.POST("/auth/login", s.handleLogin)
		api.GET("/auth/me", s.handleGetMe)
		api.GET("/auth/users", s.listUsers)
		api.POST("/auth/users", s.createUser)
		api.DELETE("/auth/users/:username", s.deleteUser)
		api.GET("/auth/keys", s.listAPIKeys)
		api.POST("/auth/keys", s.createAPIKey)
		api.DELETE("/auth/keys/:key", s.deleteAPIKey)

		// Approvals
		api.GET("/approvals/pending", s.listPendingApprovals)
		api.GET("/approvals", s.listApprovals)
		api.POST("/approvals/:id/decide", s.decideApproval)

		// Secrets vault
		api.GET("/vault/secrets", s.listVaultSecrets)
		api.POST("/vault/secrets", s.createVaultSecret)
		api.DELETE("/vault/secrets/:key", s.deleteVaultSecret)

		// Audits export
		api.GET("/audits/export", s.exportAuditCompliancePackage)

		// Connectors catalog
		api.GET("/connectors", s.listConnectors)

		// Workflows CRUD
		api.GET("/workflows", s.listWorkflows)
		api.POST("/workflows", s.createWorkflow)
		api.GET("/workflows/:id", s.getWorkflow)
		api.PUT("/workflows/:id", s.updateWorkflow)
		api.DELETE("/workflows/:id", s.deleteWorkflow)
		api.POST("/workflows/:id/execute", execLimiter, s.executeWorkflow)
		api.POST("/workflows/:id/dry-run", s.dryRunWorkflow)

		// Executions History
		api.GET("/executions", s.listExecutions)
		api.GET("/executions/:id", s.getExecution)
		api.POST("/executions/:id/resume", s.resumeExecution)
		// Signals API
		api.POST("/executions/:id/signal", s.sendSignal)
		api.GET("/executions/:id/signals", s.listExecutionSignals)
		api.GET("/executions/:id/children", s.listChildExecutions)

		// Workflow Versioning
		api.GET("/workflows/:id/versions", s.listWorkflowVersions)
		api.POST("/workflows/:id/versions/:version/activate", s.activateWorkflowVersion)

		// Dead Letter Queue (DLQ)
		api.GET("/dlq", s.listDLQMessages)
		api.GET("/dlq/:id", s.getDLQMessage)
		api.POST("/dlq/:id/replay", s.replayDLQMessage)
		api.DELETE("/dlq/:id", s.deleteDLQMessage)

		// Distributed Task Queues (External Worker Activity Polling)
		api.GET("/tasks", s.listTasks)
		api.POST("/task-queues/:queue/poll", s.pollTask)
		api.POST("/tasks/:id/heartbeat", s.heartbeatTask)
		api.POST("/tasks/:id/complete", s.completeTask)
		api.POST("/tasks/:id/fail", s.failTask)

		// Real-time In-Flight Synchronous Workflow Queries
		api.POST("/executions/:id/query", s.queryExecution)

		// Distributed Scheduled Cron Workflows
		api.GET("/cron/schedules", s.listCronSchedules)
		api.POST("/cron/schedules", s.createCronSchedule)

		// Webhook Route Bindings
		api.GET("/webhook-bindings", s.listWebhookBindings)
		api.POST("/webhook-bindings", s.createWebhookBinding)
		api.DELETE("/webhook-bindings/:id", s.deleteWebhookBinding)

		// Dynamic Inbound Webhooks Router
		api.Any("/webhooks/*path", s.handleWebhookTrigger)

		// Audit Log
		api.GET("/audits", s.listAudits)
	}

	// 4. Embedded / Static UI Frontend
	if s.config.StaticFS != nil {
		s.router.NoRoute(gin.WrapH(http.FileServer(http.FS(s.config.StaticFS))))
	}
}

func (s *Server) listConnectors(c *gin.Context) {
	descriptors := s.registry.ListDescriptors()
	c.JSON(http.StatusOK, gin.H{
		"connectors": descriptors,
		"count":      len(descriptors),
	})
}

func (s *Server) listWorkflows(c *gin.Context) {
	tenantID := c.GetString("tenant_id")
	if qTenant := c.Query("tenant_id"); qTenant != "" {
		tenantID = qTenant
	}
	if c.Query("all_tenants") == "true" {
		tenantID = ""
	}

	filter := storage.WorkflowFilter{
		TenantID: tenantID,
		Search:   c.Query("search"),
		Tag:      c.Query("tag"),
		Status:   model.WorkflowStatus(c.Query("status")),
	}

	wfs, total, err := s.store.Workflows().List(c.Request.Context(), filter)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"workflows": wfs,
		"total":     total,
	})
}

func (s *Server) createWorkflow(c *gin.Context) {
	var wf model.Workflow

	// Support both JSON and YAML input
	contentType := c.GetHeader("Content-Type")
	if contentType == "application/x-yaml" || contentType == "text/yaml" {
		body, err := c.GetRawData()
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "failed to read yaml body"})
			return
		}
		if err := yaml.Unmarshal(body, &wf); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid yaml: " + err.Error()})
			return
		}
	} else {
		if err := c.ShouldBindJSON(&wf); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
	}

	if wf.ID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "workflow id is required"})
		return
	}

	if wf.TenantID == "" {
		wf.TenantID = c.GetString("tenant_id")
	}
	if wf.TenantID == "" {
		wf.TenantID = security.DefaultTenant
	}

	if wf.Status == "" {
		wf.Status = model.WorkflowStatusActive
	}
	now := time.Now()
	wf.CreatedAt = now
	wf.UpdatedAt = now

	if err := s.store.Workflows().Create(c.Request.Context(), &wf); err != nil {
		c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
		return
	}

	_ = s.store.Audits().Log(c.Request.Context(), &model.AuditLogEntry{
		ID:         time.Now().Format("20060102150405"),
		Timestamp:  now,
		Actor:      "api_user",
		Action:     "CREATE_WORKFLOW",
		Resource:   "workflow",
		ResourceID: wf.ID,
	})

	c.JSON(http.StatusCreated, wf)
}

func (s *Server) getWorkflow(c *gin.Context) {
	id := c.Param("id")
	wf, err := s.store.Workflows().Get(c.Request.Context(), id)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "workflow not found"})
		return
	}

	format := c.Query("format")
	if format == "yaml" {
		yamlData, err := yaml.Marshal(wf)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "yaml serialization error"})
			return
		}
		c.Data(http.StatusOK, "application/x-yaml", yamlData)
		return
	}

	c.JSON(http.StatusOK, wf)
}

func (s *Server) updateWorkflow(c *gin.Context) {
	id := c.Param("id")
	var wf model.Workflow
	if err := c.ShouldBindJSON(&wf); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	wf.ID = id
	wf.UpdatedAt = time.Now()

	if err := s.store.Workflows().Update(c.Request.Context(), &wf); err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, wf)
}

func (s *Server) deleteWorkflow(c *gin.Context) {
	id := c.Param("id")
	if err := s.store.Workflows().Delete(c.Request.Context(), id); err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"deleted": true, "id": id})
}

func (s *Server) executeWorkflow(c *gin.Context) {
	id := c.Param("id")
	var payload map[string]interface{}
	if err := c.ShouldBindJSON(&payload); err != nil {
		payload = make(map[string]interface{})
	}

	actor := "operator"
	if authHeader := c.GetHeader("Authorization"); authHeader != "" {
		parts := strings.Split(authHeader, " ")
		if len(parts) == 2 {
			if claims, err := s.auth.ValidateToken(parts[1]); err == nil {
				actor = claims.Subject
			}
		}
	} else if apiKey := c.GetHeader("X-API-Key"); apiKey != "" {
		if role, ok := s.auth.ValidateAPIKey(apiKey); ok {
			actor = fmt.Sprintf("api-key:%s", role)
		}
	}

	clientIP := c.ClientIP()
	if clientIP == "" {
		clientIP = "127.0.0.1"
	}

	// Consistent Hash Ring cluster sharding
	if s.hashRing != nil && s.hashRing.NodeCount() > 1 && c.GetHeader("X-OpenFlow-Sharded") != "true" {
		routingKey := id
		if idempKey := c.GetHeader("X-Idempotency-Key"); idempKey != "" {
			routingKey = id + ":" + idempKey
		}
		targetNode, err := s.hashRing.GetNode(routingKey)
		if err == nil && targetNode != "" && targetNode != s.config.NodeID {
			s.proxyToNode(c, targetNode, payload)
			return
		}
	}
	c.Writer.Header().Set("X-OpenFlow-Sharded", "true")
	if s.config.NodeID != "" {
		c.Writer.Header().Set("X-OpenFlow-Node", s.config.NodeID)
	}

	// Use background context detached from HTTP request cancellation so asynchronous workflow execution finishes cleanly
	tenantID := c.GetString("tenant_id")
	bgCtx := security.WithTenant(context.Background(), tenantID)
	exec, err := s.engine.Execute(bgCtx, id, payload, model.TriggerTypeManual)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	_ = s.store.Audits().Log(bgCtx, &model.AuditLogEntry{
		ID:         fmt.Sprintf("aud_exec_%d", time.Now().UnixNano()),
		Timestamp:  time.Now(),
		Actor:      actor,
		Action:     "EXECUTE_WORKFLOW",
		Resource:   "workflow",
		ResourceID: id,
		ClientIP:   clientIP,
		Details: map[string]interface{}{
			"execution_id": exec.ID,
			"trigger_type": "manual",
			"payload":      payload,
		},
	})

	c.JSON(http.StatusAccepted, exec)
}

func (s *Server) proxyToNode(c *gin.Context, targetNode string, payload map[string]interface{}) {
	targetURL := targetNode
	if !strings.HasPrefix(targetURL, "http://") && !strings.HasPrefix(targetURL, "https://") {
		targetURL = "http://" + targetURL
	}
	targetURL = strings.TrimRight(targetURL, "/") + c.Request.RequestURI

	bodyBytes, err := json.Marshal(payload)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to encode proxy payload"})
		return
	}

	req, err := http.NewRequestWithContext(c.Request.Context(), c.Request.Method, targetURL, bytes.NewReader(bodyBytes))
	if err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"error": fmt.Sprintf("failed to build proxy request: %v", err)})
		return
	}

	for k, vv := range c.Request.Header {
		for _, v := range vv {
			req.Header.Add(k, v)
		}
	}
	req.Header.Set("X-OpenFlow-Sharded", "true")
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"error": fmt.Sprintf("proxy forwarding to node '%s' failed: %v", targetNode, err)})
		return
	}
	defer resp.Body.Close()

	for k, vv := range resp.Header {
		for _, v := range vv {
			c.Writer.Header().Add(k, v)
		}
	}
	c.Writer.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(c.Writer, resp.Body)
}

func (s *Server) handleWebhookTrigger(c *gin.Context) {
	rawParam := c.Param("path")
	cleanPath := strings.Trim(rawParam, "/")
	cleanPath = strings.TrimPrefix(cleanPath, "webhooks/")

	activeWfs, _, err := s.store.Workflows().List(c.Request.Context(), storage.WorkflowFilter{
		Status: model.WorkflowStatusActive,
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to list workflows: " + err.Error()})
		return
	}

	var matchedWf *model.Workflow

	// 1. Check dynamic Webhook Bindings store first
	if binding, err := s.store.Webhooks().FindByPath(c.Request.Context(), cleanPath, c.Request.Method); err == nil && binding != nil {
		if wf, err := s.store.Workflows().Get(c.Request.Context(), binding.WorkflowID); err == nil && wf != nil {
			// Validate secret token if configured
			if binding.SecretToken != "" {
				authHeader := c.GetHeader("X-Webhook-Secret")
				if authHeader == "" {
					authHeader = c.Query("secret")
				}
				if authHeader != binding.SecretToken {
					c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid webhook secret token"})
					return
				}
			}
			matchedWf = wf
		}
	}

	// 2. Fallback to inline workflow Trigger.Path definitions
	if matchedWf == nil {
		for _, w := range activeWfs {
			if w.Trigger != nil && w.Trigger.Type == model.TriggerTypeWebhook {
				tPath := strings.Trim(w.Trigger.Path, "/")
				tPath = strings.TrimPrefix(tPath, "webhooks/")
				if tPath == cleanPath {
					matchedWf = w
					break
				}
			}
		}
	}

	if matchedWf == nil {
		c.JSON(http.StatusNotFound, gin.H{
			"error":        "no active workflow found for webhook path: /" + cleanPath,
			"webhook_path": cleanPath,
		})
		return
	}

	var bodyPayload map[string]interface{}
	if err := c.ShouldBindJSON(&bodyPayload); err != nil {
		bodyPayload = make(map[string]interface{})
	}

	headers := make(map[string]string)
	for k, v := range c.Request.Header {
		if len(v) > 0 {
			headers[k] = v[0]
		}
	}
	queryParams := make(map[string]string)
	for k, v := range c.Request.URL.Query() {
		if len(v) > 0 {
			queryParams[k] = v[0]
		}
	}

	input := map[string]interface{}{
		"payload":     bodyPayload,
		"headers":     headers,
		"query":       queryParams,
		"method":      c.Request.Method,
		"path":        cleanPath,
		"client_ip":   c.ClientIP(),
		"received_at": time.Now().Format(time.RFC3339),
	}

	if idempKey := c.GetHeader("X-Idempotency-Key"); idempKey != "" {
		input["__idempotency_key"] = idempKey
	}

	exec, err := s.engine.Execute(c.Request.Context(), matchedWf.ID, input, model.TriggerTypeWebhook)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to trigger workflow: " + err.Error()})
		return
	}

	_ = s.store.Audits().Log(c.Request.Context(), &model.AuditLogEntry{
		ID:         fmt.Sprintf("aud_wh_%d", time.Now().UnixNano()),
		Timestamp:  time.Now(),
		Actor:      "webhook:" + c.ClientIP(),
		Action:     "WEBHOOK_TRIGGERED",
		Resource:   "workflow",
		ResourceID: matchedWf.ID,
		ClientIP:   c.ClientIP(),
		Details: map[string]interface{}{
			"path":         cleanPath,
			"execution_id": exec.ID,
		},
	})

	c.JSON(http.StatusAccepted, gin.H{
		"status":       "ACCEPTED",
		"message":      "Webhook received and workflow execution started",
		"workflow_id":  matchedWf.ID,
		"execution_id": exec.ID,
		"trigger_type": "webhook",
	})
}

func (s *Server) listExecutions(c *gin.Context) {
	tenantID := c.GetString("tenant_id")
	if qTenant := c.Query("tenant_id"); qTenant != "" {
		tenantID = qTenant
	}
	if c.Query("all_tenants") == "true" {
		tenantID = ""
	}

	filter := storage.ExecutionFilter{
		TenantID:   tenantID,
		WorkflowID: c.Query("workflow_id"),
		Status:     model.ExecutionStatus(c.Query("status")),
	}

	execs, total, err := s.store.Executions().List(c.Request.Context(), filter)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"executions": execs,
		"total":      total,
	})
}

func (s *Server) getExecution(c *gin.Context) {
	id := c.Param("id")
	exec, err := s.store.Executions().Get(c.Request.Context(), id)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "execution not found"})
		return
	}
	c.JSON(http.StatusOK, exec)
}

func (s *Server) resumeExecution(c *gin.Context) {
	id := c.Param("id")
	exec, err := s.engine.ResumeExecution(c.Request.Context(), id)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"message":      "execution resumption initiated",
		"execution_id": exec.ID,
		"status":       exec.Status,
	})
}

func (s *Server) getMerkleProof(c *gin.Context) {
	id := c.Param("id")
	exec, err := s.store.Executions().Get(c.Request.Context(), id)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "execution not found"})
		return
	}

	valid, verifiedRoot, verifyErr := security.VerifyChainIntegrity(exec)
	errStr := ""
	if verifyErr != nil {
		errStr = verifyErr.Error()
	}

	c.JSON(http.StatusOK, gin.H{
		"execution_id":  exec.ID,
		"merkle_root":   exec.MerkleRoot,
		"verified_root": verifiedRoot,
		"is_valid":      valid,
		"total_steps":   len(exec.Steps),
		"tamper_error":  errStr,
	})
}

func (s *Server) listAudits(c *gin.Context) {
	entries, total, err := s.store.Audits().List(c.Request.Context(), 100, 0)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"audits": entries,
		"total":  total,
	})
}

// Router returns the Gin engine router instance.
func (s *Server) Router() *gin.Engine {
	return s.router
}

// ListenAndServe starts the HTTP server.
func (s *Server) ListenAndServe() error {
	port := s.config.Port
	if port == "" {
		port = ":8080"
	}
	return s.router.Run(port)
}

func (s *Server) handleLogin(c *gin.Context) {
	var req struct {
		Username string `json:"username" binding:"required"`
		Password string `json:"password" binding:"required"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "username and password are required"})
		return
	}

	user, token, err := s.auth.AuthenticateUser(req.Username, req.Password)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"token": token,
		"user": gin.H{
			"username":  user.Username,
			"full_name": user.FullName,
			"role":      user.Role,
		},
	})
}

func (s *Server) handleGetMe(c *gin.Context) {
	authHeader := c.GetHeader("Authorization")
	if len(authHeader) > 7 && authHeader[:7] == "Bearer " {
		tokenStr := authHeader[7:]
		claims, err := s.auth.ValidateToken(tokenStr)
		if err == nil {
			c.JSON(http.StatusOK, gin.H{
				"username":  claims.Subject,
				"full_name": claims.FullName,
				"role":      claims.Role,
			})
			return
		}
	}

	c.JSON(http.StatusUnauthorized, gin.H{"error": "unauthorized"})
}

func (s *Server) listUsers(c *gin.Context) {
	users := s.auth.ListUsers()
	c.JSON(http.StatusOK, gin.H{
		"users": users,
		"count": len(users),
	})
}

func (s *Server) createUser(c *gin.Context) {
	var req struct {
		Username string        `json:"username" binding:"required"`
		Password string        `json:"password" binding:"required"`
		FullName string        `json:"full_name"`
		Role     security.Role `json:"role"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	err := s.auth.CreateUser(req.Username, req.Password, req.FullName, req.Role)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusCreated, gin.H{
		"message":  "user created successfully",
		"username": req.Username,
		"role":     req.Role,
	})
}

func (s *Server) deleteUser(c *gin.Context) {
	username := c.Param("username")
	if err := s.auth.DeleteUser(username); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"deleted": true, "username": username})
}

func (s *Server) listAPIKeys(c *gin.Context) {
	keys := s.auth.ListAPIKeys()
	c.JSON(http.StatusOK, gin.H{
		"keys":  keys,
		"count": len(keys),
	})
}

func (s *Server) createAPIKey(c *gin.Context) {
	var req struct {
		Name          string        `json:"name"`
		Role          security.Role `json:"role" binding:"required"`
		ExpiresInDays int           `json:"expires_in_days"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	meta, rawKey, err := s.auth.GenerateAPIKey(req.Name, req.Role, req.ExpiresInDays)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusCreated, gin.H{
		"message":  "API Key generated successfully. Copy it now, it will not be displayed again.",
		"api_key":  rawKey,
		"metadata": meta,
	})
}

func (s *Server) deleteAPIKey(c *gin.Context) {
	keyID := c.Param("key")
	if err := s.auth.RevokeAPIKey(keyID); err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"deleted": true, "key_id": keyID})
}

// Approval Gate Handlers
func (s *Server) listPendingApprovals(c *gin.Context) {
	list, err := s.store.Approvals().ListPending(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusOK, gin.H{"approvals": []*model.ApprovalRequest{}, "count": 0, "warning": err.Error()})
		return
	}
	if list == nil {
		list = make([]*model.ApprovalRequest, 0)
	}
	c.JSON(http.StatusOK, gin.H{
		"approvals": list,
		"count":     len(list),
	})
}

func (s *Server) listApprovals(c *gin.Context) {
	list, total, err := s.store.Approvals().List(c.Request.Context(), 50, 0)
	if err != nil {
		c.JSON(http.StatusOK, gin.H{"approvals": []*model.ApprovalRequest{}, "total": 0, "warning": err.Error()})
		return
	}
	if list == nil {
		list = make([]*model.ApprovalRequest, 0)
	}
	c.JSON(http.StatusOK, gin.H{
		"approvals": list,
		"total":     total,
	})
}

func (s *Server) decideApproval(c *gin.Context) {
	approvalID := c.Param("id")
	var req struct {
		Decision string `json:"decision" binding:"required"` // "APPROVE" | "REJECT"
		Reason   string `json:"reason"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	actor := "operator"
	if authHeader := c.GetHeader("Authorization"); authHeader != "" {
		parts := strings.Split(authHeader, " ")
		if len(parts) == 2 {
			if claims, err := s.auth.ValidateToken(parts[1]); err == nil {
				actor = claims.Subject
			}
		}
	}

	approved := strings.ToUpper(req.Decision) == "APPROVE" || strings.ToUpper(req.Decision) == "APPROVED"
	err := s.engine.ResumeApproval(c.Request.Context(), approvalID, approved, actor, req.Reason)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	actionName := "APPROVE_TRANSACTION"
	if !approved {
		actionName = "REJECT_TRANSACTION"
	}

	_ = s.store.Audits().Log(c.Request.Context(), &model.AuditLogEntry{
		ID:         fmt.Sprintf("aud_appr_%d", time.Now().UnixNano()),
		Timestamp:  time.Now(),
		Actor:      actor,
		Action:     actionName,
		Resource:   "approval",
		ResourceID: approvalID,
		ClientIP:   c.ClientIP(),
		Details: map[string]interface{}{
			"decision": req.Decision,
			"reason":   req.Reason,
		},
	})

	c.JSON(http.StatusOK, gin.H{
		"message":     "Approval decided successfully",
		"approval_id": approvalID,
		"decision":    req.Decision,
		"decided_by":  actor,
	})
}

// Vault Handlers
func (s *Server) listVaultSecrets(c *gin.Context) {
	if security.GlobalVault == nil {
		c.JSON(http.StatusOK, gin.H{"secrets": []string{}})
		return
	}
	keys := security.GlobalVault.ListKeys()
	c.JSON(http.StatusOK, gin.H{
		"secrets": keys,
		"count":   len(keys),
	})
}

func (s *Server) createVaultSecret(c *gin.Context) {
	var req struct {
		Key   string `json:"key" binding:"required"`
		Value string `json:"value" binding:"required"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if security.GlobalVault == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "vault not initialized"})
		return
	}

	if err := security.GlobalVault.SetSecret(req.Key, req.Value); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusCreated, gin.H{
		"message": "Secret stored securely in AES-256-GCM vault",
		"key":     req.Key,
	})
}

func (s *Server) deleteVaultSecret(c *gin.Context) {
	key := c.Param("key")
	if security.GlobalVault == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "vault not initialized"})
		return
	}

	if err := security.GlobalVault.DeleteSecret(key); err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"deleted": true, "key": key})
}

// Audit compliance package export
func (s *Server) exportAuditCompliancePackage(c *gin.Context) {
	ctx := c.Request.Context()
	audits, total, err := s.store.Audits().List(ctx, 1000, 0)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	compliancePackage := gin.H{
		"generated_at":  time.Now().UTC().Format(time.RFC3339),
		"total_records": total,
		"records":       audits,
	}

	c.Header("Content-Disposition", fmt.Sprintf("attachment; filename=openflow_audit_package_%d.json", time.Now().Unix()))
	c.JSON(http.StatusOK, compliancePackage)
}

// Signals and child workflow handlers

type SignalRequest struct {
	Name    string                 `json:"name" binding:"required"`
	Payload map[string]interface{} `json:"payload"`
}

func (s *Server) sendSignal(c *gin.Context) {
	execID := c.Param("id")
	var req SignalRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "signal 'name' is required"})
		return
	}

	err := s.engine.ResumeSignal(c.Request.Context(), execID, req.Name, req.Payload)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	_ = s.store.Audits().Log(c.Request.Context(), &model.AuditLogEntry{
		ID:         fmt.Sprintf("aud_sig_%d", time.Now().UnixNano()),
		Timestamp:  time.Now(),
		Actor:      "api_user",
		Action:     "SIGNAL_INJECTED",
		Resource:   "execution",
		ResourceID: execID,
		ClientIP:   c.ClientIP(),
		Details: map[string]interface{}{
			"signal_name": req.Name,
			"payload":     req.Payload,
		},
	})

	c.JSON(http.StatusOK, gin.H{
		"status":      "SIGNAL_DELIVERED",
		"signal_name": req.Name,
		"execution_id": execID,
	})
}

func (s *Server) listExecutionSignals(c *gin.Context) {
	execID := c.Param("id")
	signals, err := s.store.Signals().ListByExecution(c.Request.Context(), execID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"signals": signals})
}

func (s *Server) listChildExecutions(c *gin.Context) {
	execID := c.Param("id")
	if strings.HasPrefix(execID, "sim_") {
		c.JSON(http.StatusOK, gin.H{"children": []*model.Execution{}, "count": 0})
		return
	}
	children, err := s.store.Executions().ListChildren(c.Request.Context(), execID)
	if err != nil {
		c.JSON(http.StatusOK, gin.H{"children": []*model.Execution{}, "count": 0, "warning": err.Error()})
		return
	}
	if children == nil {
		children = make([]*model.Execution, 0)
	}
	c.JSON(http.StatusOK, gin.H{"children": children, "count": len(children)})
}

// Workflow versioning handlers

func (s *Server) listWorkflowVersions(c *gin.Context) {
	wfID := c.Param("id")
	versions, err := s.store.WorkflowVersions().ListVersions(c.Request.Context(), wfID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"versions": versions})
}

func (s *Server) activateWorkflowVersion(c *gin.Context) {
	wfID := c.Param("id")
	versionStr := c.Param("version")
	var verNum int
	if _, err := fmt.Sscanf(versionStr, "%d", &verNum); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid version number"})
		return
	}

	err := s.store.WorkflowVersions().SetActiveVersion(c.Request.Context(), wfID, verNum)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"status": "ACTIVATED", "workflow_id": wfID, "version": verNum})
}

// Dry run and DLQ handlers

func (s *Server) dryRunWorkflow(c *gin.Context) {
	id := c.Param("id")
	var req struct {
		Input          map[string]interface{} `json:"input"`
		MockOverrides  map[string]interface{} `json:"mock_overrides"`
		MockConnectors bool                   `json:"mock_connectors"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		req.Input = make(map[string]interface{})
	}

	simExec, err := s.engine.DryRun(c.Request.Context(), id, req.Input, req.MockOverrides)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, simExec)
}

func (s *Server) listDLQMessages(c *gin.Context) {
	limit := 50
	offset := 0
	if l := c.Query("limit"); l != "" {
		fmt.Sscanf(l, "%d", &limit)
	}
	if o := c.Query("offset"); o != "" {
		fmt.Sscanf(o, "%d", &offset)
	}

	filter := storage.DLQFilter{
		Status:     model.DLQStatus(c.Query("status")),
		WorkflowID: c.Query("workflow_id"),
		Source:     c.Query("source"),
		Limit:      limit,
		Offset:     offset,
	}

	messages, total, err := s.store.DLQ().List(c.Request.Context(), filter)
	if err != nil {
		c.JSON(http.StatusOK, gin.H{"messages": []*model.DLQMessage{}, "total": 0, "limit": limit, "offset": offset, "warning": err.Error()})
		return
	}
	if messages == nil {
		messages = make([]*model.DLQMessage, 0)
	}

	c.JSON(http.StatusOK, gin.H{
		"messages": messages,
		"total":    total,
		"limit":    limit,
		"offset":   offset,
	})
}

func (s *Server) getDLQMessage(c *gin.Context) {
	id := c.Param("id")
	msg, err := s.store.DLQ().Get(c.Request.Context(), id)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "dlq message not found"})
		return
	}
	c.JSON(http.StatusOK, msg)
}

func (s *Server) replayDLQMessage(c *gin.Context) {
	id := c.Param("id")
	msg, err := s.store.DLQ().Get(c.Request.Context(), id)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "dlq message not found"})
		return
	}

	// Trigger execution with stored payload
	triggerType := model.TriggerTypeManual
	if msg.Source == "webhook" {
		triggerType = model.TriggerTypeWebhook
	}

	exec, err := s.engine.Execute(c.Request.Context(), msg.WorkflowID, msg.Payload, triggerType)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to replay: " + err.Error()})
		return
	}

	now := time.Now()
	_ = s.store.DLQ().UpdateStatus(c.Request.Context(), id, model.DLQStatusReplayed, &now)

	_ = s.store.Audits().Log(c.Request.Context(), &model.AuditLogEntry{
		ID:         fmt.Sprintf("aud_dlq_rep_%d", time.Now().UnixNano()),
		Timestamp:  time.Now(),
		Actor:      "api_operator",
		Action:     "DLQ_REPLAYED",
		Resource:   "dlq_message",
		ResourceID: id,
		ClientIP:   c.ClientIP(),
		Details: map[string]interface{}{
			"workflow_id":  msg.WorkflowID,
			"execution_id": exec.ID,
		},
	})

	c.JSON(http.StatusOK, gin.H{
		"status":       "REPLAYED",
		"dlq_id":       id,
		"execution_id": exec.ID,
		"workflow_id":  msg.WorkflowID,
	})
}

func (s *Server) deleteDLQMessage(c *gin.Context) {
	id := c.Param("id")
	err := s.store.DLQ().Delete(c.Request.Context(), id)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "DELETED", "id": id})
}

// Webhook bindings

func (s *Server) listWebhookBindings(c *gin.Context) {
	bindings, err := s.store.Webhooks().List(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusOK, gin.H{"bindings": []*model.WebhookBinding{}, "warning": err.Error()})
		return
	}
	if bindings == nil {
		bindings = make([]*model.WebhookBinding, 0)
	}
	c.JSON(http.StatusOK, gin.H{"bindings": bindings})
}

func (s *Server) createWebhookBinding(c *gin.Context) {
	var req model.WebhookBinding
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if req.PathPattern == "" || req.WorkflowID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "path_pattern and workflow_id are required"})
		return
	}
	if req.Method == "" {
		req.Method = "*"
	}
	req.Enabled = true

	if err := s.store.Webhooks().Create(c.Request.Context(), &req); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusCreated, req)
}

func (s *Server) deleteWebhookBinding(c *gin.Context) {
	id := c.Param("id")
	if err := s.store.Webhooks().Delete(c.Request.Context(), id); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "DELETED", "id": id})
}

// Task queue handlers

func (s *Server) pollTask(c *gin.Context) {
	queue := c.Param("queue")
	workerID := c.Query("worker_id")
	if workerID == "" {
		workerID = fmt.Sprintf("worker_%s", c.ClientIP())
	}

	timeoutSec := 20
	if t := c.Query("timeout"); t != "" {
		fmt.Sscanf(t, "%d", &timeoutSec)
	}

	task, err := s.engine.TaskQueues().Poll(c.Request.Context(), queue, workerID, time.Duration(timeoutSec)*time.Second)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if task == nil {
		c.Status(http.StatusNoContent)
		return
	}

	c.JSON(http.StatusOK, task)
}

func (s *Server) heartbeatTask(c *gin.Context) {
	id := c.Param("id")
	var req struct {
		WorkerID  string `json:"worker_id" binding:"required"`
		LockToken string `json:"lock_token" binding:"required"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if err := s.engine.TaskQueues().Heartbeat(c.Request.Context(), id, req.WorkerID, req.LockToken); err != nil {
		c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "HEARTBEAT_RECORDED", "task_id": id})
}

func (s *Server) completeTask(c *gin.Context) {
	id := c.Param("id")
	var req struct {
		WorkerID  string                 `json:"worker_id" binding:"required"`
		LockToken string                 `json:"lock_token" binding:"required"`
		Output    map[string]interface{} `json:"output"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if err := s.engine.TaskQueues().Complete(c.Request.Context(), id, req.WorkerID, req.LockToken, req.Output); err != nil {
		c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "COMPLETED", "task_id": id})
}

func (s *Server) failTask(c *gin.Context) {
	id := c.Param("id")
	var req struct {
		WorkerID     string `json:"worker_id" binding:"required"`
		LockToken    string `json:"lock_token" binding:"required"`
		ErrorMessage string `json:"error_message" binding:"required"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if err := s.engine.TaskQueues().Fail(c.Request.Context(), id, req.WorkerID, req.LockToken, req.ErrorMessage); err != nil {
		c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "FAILED", "task_id": id})
}

func (s *Server) listTasks(c *gin.Context) {
	queue := c.Query("queue")
	status := model.TaskStatus(c.Query("status"))

	tasks, err := s.store.TaskQueues().List(c.Request.Context(), queue, status)
	if err != nil {
		c.JSON(http.StatusOK, gin.H{"tasks": []*model.TaskItem{}, "warning": err.Error()})
		return
	}
	if tasks == nil {
		tasks = make([]*model.TaskItem, 0)
	}
	c.JSON(http.StatusOK, gin.H{"tasks": tasks})
}

// Workflow query handlers

func (s *Server) queryExecution(c *gin.Context) {
	id := c.Param("id")
	var req struct {
		Query string `json:"query"`
	}
	_ = c.ShouldBindJSON(&req)

	result, err := s.engine.Query(c.Request.Context(), id, req.Query)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"execution_id": id, "result": result})
}

// Cron schedule handlers

func (s *Server) listCronSchedules(c *gin.Context) {
	wfs, _, err := s.store.Workflows().List(c.Request.Context(), storage.WorkflowFilter{Status: model.WorkflowStatusActive})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	schedules := make([]gin.H, 0)
	for _, wf := range wfs {
		if wf.CronSchedule != "" {
			schedules = append(schedules, gin.H{
				"workflow_id":    wf.ID,
				"workflow_name":  wf.Name,
				"cron_schedule":  wf.CronSchedule,
				"overlap_policy": wf.OverlapPolicy,
			})
		}
	}
	c.JSON(http.StatusOK, gin.H{"schedules": schedules})
}

func (s *Server) createCronSchedule(c *gin.Context) {
	var req struct {
		WorkflowID    string              `json:"workflow_id" binding:"required"`
		CronSchedule  string              `json:"cron_schedule" binding:"required"`
		OverlapPolicy model.OverlapPolicy `json:"overlap_policy"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	wf, err := s.store.Workflows().Get(c.Request.Context(), req.WorkflowID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "workflow not found"})
		return
	}

	if req.OverlapPolicy == "" {
		req.OverlapPolicy = model.OverlapPolicySkip
	}

	wf.CronSchedule = req.CronSchedule
	wf.OverlapPolicy = req.OverlapPolicy
	_ = s.store.Workflows().Update(c.Request.Context(), wf)

	err = s.engine.CronScheduler().RegisterSchedule(wf.ID, wf.CronSchedule, wf.OverlapPolicy)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"status":         "SCHEDULED",
		"workflow_id":    wf.ID,
		"cron_schedule":  wf.CronSchedule,
		"overlap_policy": wf.OverlapPolicy,
	})
}
