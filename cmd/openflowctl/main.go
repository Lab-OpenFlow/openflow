package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"

	"github.com/Lab-OpenFlow/openflow/pkg/k8s"
	"github.com/Lab-OpenFlow/openflow/pkg/model"
)

var (
	serverURL string
	apiKey    string
)

func main() {
	var rootCmd = &cobra.Command{
		Use:   "openflowctl",
		Short: "OpenFlow CLI - Manage workflows, triggers, executions, and Kubernetes GitOps",
		Long:  `OpenFlow CLI is the official command-line tool for the OpenFlow distributed workflow orchestrator platform.`,
	}

	rootCmd.PersistentFlags().StringVar(&serverURL, "server", "http://localhost:8080", "OpenFlow API Server URL")
	rootCmd.PersistentFlags().StringVar(&apiKey, "api-key", "openflow-master-key", "Authentication API Key")

	// 1. WORKFLOW COMMANDS
	var workflowCmd = &cobra.Command{
		Use:   "workflow",
		Short: "Manage workflow definitions",
	}

	var applyFile string
	var applyCmd = &cobra.Command{
		Use:   "apply",
		Short: "Deploy or update a workflow from a YAML file",
		Run: func(cmd *cobra.Command, args []string) {
			if applyFile == "" {
				fmt.Println("Error: --file (-f) flag is required")
				os.Exit(1)
			}

			data, err := os.ReadFile(applyFile)
			if err != nil {
				fmt.Printf("Error reading file: %v\n", err)
				os.Exit(1)
			}

			var wf model.Workflow
			if err := yaml.Unmarshal(data, &wf); err != nil {
				fmt.Printf("Error parsing YAML: %v\n", err)
				os.Exit(1)
			}

			jsonData, err := json.Marshal(wf)
			if err != nil {
				fmt.Printf("Error encoding workflow to JSON: %v\n", err)
				os.Exit(1)
			}

			req, err := http.NewRequest("POST", serverURL+"/api/v1/workflows", bytes.NewReader(jsonData))
			if err != nil {
				fmt.Printf("Error creating request: %v\n", err)
				os.Exit(1)
			}
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("X-API-Key", apiKey)

			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				fmt.Printf("Connection error: %v\n", err)
				os.Exit(1)
			}
			defer resp.Body.Close()

			body, _ := io.ReadAll(resp.Body)
			if resp.StatusCode >= 400 {
				fmt.Printf("Server rejected request (status %d): %s\n", resp.StatusCode, string(body))
				os.Exit(1)
			}

			var prettyJSON bytes.Buffer
			_ = json.Indent(&prettyJSON, body, "", "  ")
			fmt.Printf("✔ Workflow deployed successfully:\n%s\n", prettyJSON.String())
		},
	}
	applyCmd.Flags().StringVarP(&applyFile, "file", "f", "", "Path to workflow YAML file")
	workflowCmd.AddCommand(applyCmd)
	rootCmd.AddCommand(applyCmd)

	var listWorkflowsCmd = &cobra.Command{
		Use:   "list",
		Short: "List all workflows registered on the server",
		Run: func(cmd *cobra.Command, args []string) {
			resp, err := http.Get(serverURL + "/api/v1/workflows")
			if err != nil {
				fmt.Printf("Error: %v\n", err)
				os.Exit(1)
			}
			defer resp.Body.Close()

			var res struct {
				Workflows []map[string]interface{} `json:"workflows"`
				Total     int                      `json:"total"`
			}
			if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
				fmt.Printf("Error parsing response: %v\n", err)
				os.Exit(1)
			}

			w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
			fmt.Fprintln(w, "ID\tNAME\tSTATUS\tSTAGES\tSAGA STRATEGY\tSTART AT")
			for _, wf := range res.Workflows {
				stagesCount := 0
				if stages, ok := wf["stages"].([]interface{}); ok {
					stagesCount = len(stages)
				}
				saga := wf["saga_strategy"]
				if saga == nil || saga == "" {
					saga = "parallel"
				}
				fmt.Fprintf(w, "%v\t%v\t%v\t%d\t%v\t%v\n",
					wf["id"], wf["name"], wf["status"], stagesCount, saga, wf["start_at"])
			}
			w.Flush()
			fmt.Printf("\nTotal: %d workflows\n", res.Total)
		},
	}
	workflowCmd.AddCommand(listWorkflowsCmd)

	var runData string
	var runPayloadFile string
	var runCmd = &cobra.Command{
		Use:   "run <workflow-id>",
		Short: "Execute a workflow with an input payload",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			wfID := args[0]
			var inputPayload []byte

			if runPayloadFile != "" {
				fileBytes, err := os.ReadFile(runPayloadFile)
				if err != nil {
					fmt.Printf("Error reading payload file: %v\n", err)
					os.Exit(1)
				}
				inputPayload = fileBytes
			} else {
				inputPayload = []byte(runData)
			}

			req, err := http.NewRequest("POST", serverURL+"/api/v1/workflows/"+wfID+"/execute", bytes.NewReader(inputPayload))
			if err != nil {
				fmt.Printf("Error creating request: %v\n", err)
				os.Exit(1)
			}
			req.Header.Set("Content-Type", "application/json")

			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				fmt.Printf("Execution request failed: %v\n", err)
				os.Exit(1)
			}
			defer resp.Body.Close()

			body, _ := io.ReadAll(resp.Body)
			if resp.StatusCode >= 400 {
				fmt.Printf("Execution failed (status %d): %s\n", resp.StatusCode, string(body))
				os.Exit(1)
			}

			var execResp map[string]interface{}
			_ = json.Unmarshal(body, &execResp)
			execID, _ := execResp["id"].(string)
			if execID == "" {
				execID, _ = execResp["execution_id"].(string)
			}

			fmt.Printf("🚀 Execution triggered: %s (workflow: %s)\n", execID, wfID)
			fmt.Println("Polling for completion...")

			// Poll execution state
			for i := 0; i < 60; i++ {
				time.Sleep(500 * time.Millisecond)
				eResp, err := http.Get(serverURL + "/api/v1/executions/" + execID)
				if err != nil {
					continue
				}

				var execData map[string]interface{}
				_ = json.NewDecoder(eResp.Body).Decode(&execData)
				eResp.Body.Close()

				status, _ := execData["status"].(string)
				if status == "COMPLETED" || status == "FAILED" || status == "COMPENSATED" {
					duration, _ := execData["duration_ms"].(float64)
					fmt.Printf("\n🏁 Execution Finished: [%s] (in %v ms)\n", status, duration)
					if errStr, ok := execData["error"].(string); ok && errStr != "" {
						fmt.Printf("❌ Error: %s\n", errStr)
					}
					return
				}
			}
		},
	}
	runCmd.Flags().StringVarP(&runData, "data", "d", "{}", "JSON input payload string")
	runCmd.Flags().StringVarP(&runPayloadFile, "file", "f", "", "JSON input payload file path")
	workflowCmd.AddCommand(runCmd)
	rootCmd.AddCommand(runCmd)

	rootCmd.AddCommand(workflowCmd)

	// 2. EXECUTION COMMANDS
	var execCmd = &cobra.Command{
		Use:   "execution",
		Short: "Inspect, query, and resume workflow executions",
	}

	var execFilterWorkflow string
	var execFilterStatus string
	var listExecsCmd = &cobra.Command{
		Use:   "list",
		Short: "List and search executions",
		Run: func(cmd *cobra.Command, args []string) {
			params := url.Values{}
			if execFilterWorkflow != "" {
				params.Set("workflow_id", execFilterWorkflow)
			}
			if execFilterStatus != "" {
				params.Set("status", strings.ToUpper(execFilterStatus))
			}

			queryURL := serverURL + "/api/v1/executions"
			if len(params) > 0 {
				queryURL += "?" + params.Encode()
			}

			resp, err := http.Get(queryURL)
			if err != nil {
				fmt.Printf("Error: %v\n", err)
				os.Exit(1)
			}
			defer resp.Body.Close()

			var res struct {
				Executions []map[string]interface{} `json:"executions"`
				Total      int                      `json:"total"`
			}
			_ = json.NewDecoder(resp.Body).Decode(&res)

			w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
			fmt.Fprintln(w, "EXECUTION ID\tWORKFLOW\tSTATUS\tDURATION\tSTARTED AT")
			for _, e := range res.Executions {
				dur := e["duration_ms"]
				started := e["started_at"]
				fmt.Fprintf(w, "%v\t%v\t%v\t%v ms\t%v\n",
					e["id"], e["workflow_name"], e["status"], dur, started)
			}
			w.Flush()
			fmt.Printf("\nTotal: %d executions\n", res.Total)
		},
	}
	listExecsCmd.Flags().StringVarP(&execFilterWorkflow, "workflow", "w", "", "Filter by workflow ID")
	listExecsCmd.Flags().StringVarP(&execFilterStatus, "status", "s", "", "Filter by status (RUNNING, COMPLETED, FAILED)")
	execCmd.AddCommand(listExecsCmd)

	var resumeExecCmd = &cobra.Command{
		Use:   "resume <execution-id>",
		Short: "Deterministically resume an interrupted workflow execution from the crash point",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			execID := args[0]
			req, err := http.NewRequest("POST", serverURL+"/api/v1/executions/"+execID+"/resume", nil)
			if err != nil {
				fmt.Printf("Error creating request: %v\n", err)
				os.Exit(1)
			}

			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				fmt.Printf("Resume request failed: %v\n", err)
				os.Exit(1)
			}
			defer resp.Body.Close()

			body, _ := io.ReadAll(resp.Body)
			if resp.StatusCode >= 400 {
				fmt.Printf("Failed to resume execution (status %d): %s\n", resp.StatusCode, string(body))
				os.Exit(1)
			}

			var res map[string]interface{}
			_ = json.Unmarshal(body, &res)
			fmt.Printf("✔ Replay recovery initiated for execution '%s' (status: %v)\n", execID, res["status"])
		},
	}
	execCmd.AddCommand(resumeExecCmd)

	var getExecCmd = &cobra.Command{
		Use:   "get <execution-id>",
		Short: "Get complete execution details, inputs, and step traces",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			execID := args[0]
			resp, err := http.Get(serverURL + "/api/v1/executions/" + execID)
			if err != nil {
				fmt.Printf("Error: %v\n", err)
				os.Exit(1)
			}
			defer resp.Body.Close()

			if resp.StatusCode == 404 {
				fmt.Printf("Execution '%s' not found.\n", execID)
				os.Exit(1)
			}

			var execData map[string]interface{}
			_ = json.NewDecoder(resp.Body).Decode(&execData)

			wfName, _ := execData["workflow_name"].(string)
			status, _ := execData["status"].(string)
			duration, _ := execData["duration_ms"].(float64)

			fmt.Printf("📋 Execution Instance: %s\n", execID)
			fmt.Printf("• Workflow: %s\n", wfName)
			fmt.Printf("• Status  : [%s]\n", status)
			fmt.Printf("• Duration: %v ms\n", duration)
			fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
			fmt.Println(" STAGE EXECUTION STEPS:")
			fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

			if steps, ok := execData["steps"].([]interface{}); ok {
				for idx, s := range steps {
					stepMap, _ := s.(map[string]interface{})
					stepID, _ := stepMap["id"].(string)
					stageName, _ := stepMap["stage_name"].(string)
					stageType, _ := stepMap["stage_type"].(string)
					stepStatus, _ := stepMap["status"].(string)
					dur, _ := stepMap["duration_ms"].(float64)
					isComp, _ := stepMap["is_compensation"].(bool)

					icon := "✔"
					if stepStatus == "FAILED" {
						icon = "✖"
					} else if stepStatus == "COMPENSATED" || stepStatus == "COMPENSATING" {
						icon = "↺"
					}

					compNote := ""
					if isComp {
						compNote = fmt.Sprintf(" [SAGA ROLLBACK for %v]", stepMap["compensates_for"])
					}

					fmt.Printf("\n %s [%d] %s (%s) %s\n", icon, idx+1, stageName, stageType, compNote)
					fmt.Printf("    • Step ID : %s\n", stepID)
					fmt.Printf("    • Status  : %s (%v ms)\n", stepStatus, dur)
				}
			}
			fmt.Println("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
		},
	}
	execCmd.AddCommand(getExecCmd)
	rootCmd.AddCommand(execCmd)

	// 3. KUBERNETES GITOPS COMMANDS
	var k8sCmd = &cobra.Command{
		Use:   "k8s",
		Short: "Kubernetes GitOps helpers for OpenFlow CRDs",
	}

	var k8sExportFile string
	var k8sNamespace string
	var k8sExportCmd = &cobra.Command{
		Use:   "export",
		Short: "Convert an OpenFlow YAML workflow into a Kubernetes Workflow CRD manifest",
		Run: func(cmd *cobra.Command, args []string) {
			if k8sExportFile == "" {
				fmt.Println("Error: --file (-f) is required")
				os.Exit(1)
			}

			data, err := os.ReadFile(k8sExportFile)
			if err != nil {
				fmt.Printf("Error reading file: %v\n", err)
				os.Exit(1)
			}

			var wf model.Workflow
			if err := yaml.Unmarshal(data, &wf); err != nil {
				fmt.Printf("Error parsing workflow YAML: %v\n", err)
				os.Exit(1)
			}

			crd := k8s.WorkflowCRD{
				APIVersion: "openflow.dev/v1alpha1",
				Kind:       "Workflow",
				Metadata: k8s.ObjectMetadata{
					Name:      wf.ID,
					Namespace: k8sNamespace,
					Labels: map[string]string{
						"app.kubernetes.io/name":       "openflow",
						"app.kubernetes.io/managed-by": "gitops",
					},
				},
				Spec: k8s.WorkflowSpec{
					Name:         wf.Name,
					Version:      wf.Version,
					Description:  wf.Description,
					StartAt:      wf.StartAt,
					SagaStrategy: string(wf.SagaStrategy),
					Variables:    wf.Variables,
					Stages:       wf.Stages,
				},
			}

			crdBytes, err := yaml.Marshal(crd)
			if err != nil {
				fmt.Printf("Error generating CRD YAML: %v\n", err)
				os.Exit(1)
			}

			fmt.Println(string(crdBytes))
		},
	}
	k8sExportCmd.Flags().StringVarP(&k8sExportFile, "file", "f", "", "Path to OpenFlow workflow YAML")
	k8sExportCmd.Flags().StringVarP(&k8sNamespace, "namespace", "n", "default", "Kubernetes target namespace")
	k8sCmd.AddCommand(k8sExportCmd)

	var k8sRunInput string
	var k8sRunCmd = &cobra.Command{
		Use:   "run <workflow-name>",
		Short: "Generate a Kubernetes WorkflowRun CRD manifest to execute a workflow",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			wfName := args[0]
			var inputMap map[string]interface{}
			_ = json.Unmarshal([]byte(k8sRunInput), &inputMap)

			runCRD := k8s.WorkflowRunCRD{
				APIVersion: "openflow.dev/v1alpha1",
				Kind:       "WorkflowRun",
				Metadata: k8s.ObjectMetadata{
					Name:      fmt.Sprintf("%s-run-%d", wfName, time.Now().Unix()),
					Namespace: k8sNamespace,
				},
				Spec: k8s.WorkflowRunSpec{
					WorkflowRef: wfName,
					Input:       inputMap,
				},
			}

			crdBytes, err := yaml.Marshal(runCRD)
			if err != nil {
				fmt.Printf("Error generating WorkflowRun YAML: %v\n", err)
				os.Exit(1)
			}

			fmt.Println(string(crdBytes))
		},
	}
	k8sRunCmd.Flags().StringVarP(&k8sRunInput, "data", "d", "{}", "JSON input payload")
	k8sRunCmd.Flags().StringVarP(&k8sNamespace, "namespace", "n", "default", "Kubernetes target namespace")
	k8sCmd.AddCommand(k8sRunCmd)

	rootCmd.AddCommand(k8sCmd)

	// Version command
	rootCmd.AddCommand(&cobra.Command{
		Use:   "version",
		Short: "Print openflowctl version",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println("openflowctl version 1.0.0")
		},
	})

	// 4. SIGNAL COMMAND
	var sigName string
	var sigPayload string
	var signalCmd = &cobra.Command{
		Use:   "signal <execution-id>",
		Short: "Inject an external event / signal into a waiting workflow execution",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			execID := args[0]
			if sigName == "" {
				fmt.Println("Error: --name flag is required")
				os.Exit(1)
			}

			payloadMap := make(map[string]interface{})
			if sigPayload != "" {
				if err := json.Unmarshal([]byte(sigPayload), &payloadMap); err != nil {
					fmt.Printf("Error: invalid JSON in --payload: %v\n", err)
					os.Exit(1)
				}
			}

			reqBody, _ := json.Marshal(map[string]interface{}{
				"name":    sigName,
				"payload": payloadMap,
			})

			req, _ := http.NewRequest("POST", serverURL+"/api/v1/executions/"+execID+"/signal", bytes.NewReader(reqBody))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("X-API-Key", apiKey)

			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				fmt.Printf("Connection error: %v\n", err)
				os.Exit(1)
			}
			defer resp.Body.Close()

			body, _ := io.ReadAll(resp.Body)
			if resp.StatusCode >= 400 {
				fmt.Printf("Signal delivery failed (status %d): %s\n", resp.StatusCode, string(body))
				os.Exit(1)
			}

			fmt.Printf("✔ Signal '%s' delivered successfully to execution '%s'\n", sigName, execID)
		},
	}
	signalCmd.Flags().StringVar(&sigName, "name", "", "Signal name (e.g. otp_confirmed)")
	signalCmd.Flags().StringVar(&sigPayload, "payload", "{}", "JSON payload data to inject")
	rootCmd.AddCommand(signalCmd)

	// 5. APPROVAL COMMANDS
	var apprRole string
	var apprReason string
	var approveCmd = &cobra.Command{
		Use:   "approve <approval-id>",
		Short: "Approve a pending Human-in-the-Loop review request",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			apprID := args[0]
			reqBody, _ := json.Marshal(map[string]interface{}{
				"decision": "APPROVE",
				"role":     apprRole,
				"reason":   apprReason,
			})

			req, _ := http.NewRequest("POST", serverURL+"/api/v1/approvals/"+apprID+"/decide", bytes.NewReader(reqBody))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("X-API-Key", apiKey)

			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				fmt.Printf("Connection error: %v\n", err)
				os.Exit(1)
			}
			defer resp.Body.Close()

			body, _ := io.ReadAll(resp.Body)
			if resp.StatusCode >= 400 {
				fmt.Printf("Approval failed (status %d): %s\n", resp.StatusCode, string(body))
				os.Exit(1)
			}

			fmt.Printf("✔ Approval '%s' marked as APPROVED\n", apprID)
		},
	}
	approveCmd.Flags().StringVar(&apprRole, "role", "APPROVER", "Actor role")
	approveCmd.Flags().StringVar(&apprReason, "reason", "Approved via openflowctl", "Decision reason")
	rootCmd.AddCommand(approveCmd)

	var rejectCmd = &cobra.Command{
		Use:   "reject <approval-id>",
		Short: "Reject a pending Human-in-the-Loop review request",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			apprID := args[0]
			reqBody, _ := json.Marshal(map[string]interface{}{
				"decision": "REJECT",
				"role":     apprRole,
				"reason":   apprReason,
			})

			req, _ := http.NewRequest("POST", serverURL+"/api/v1/approvals/"+apprID+"/decide", bytes.NewReader(reqBody))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("X-API-Key", apiKey)

			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				fmt.Printf("Connection error: %v\n", err)
				os.Exit(1)
			}
			defer resp.Body.Close()

			body, _ := io.ReadAll(resp.Body)
			if resp.StatusCode >= 400 {
				fmt.Printf("Rejection failed (status %d): %s\n", resp.StatusCode, string(body))
				os.Exit(1)
			}

			fmt.Printf("✔ Approval '%s' marked as REJECTED\n", apprID)
		},
	}
	rejectCmd.Flags().StringVar(&apprRole, "role", "APPROVER", "Actor role")
	rejectCmd.Flags().StringVar(&apprReason, "reason", "Rejected via openflowctl", "Decision reason")
	rootCmd.AddCommand(rejectCmd)

	// 6. VERSIONING COMMANDS
	var versionsCmd = &cobra.Command{
		Use:   "versions <workflow-id>",
		Short: "List immutable historical versions of a workflow",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			wfID := args[0]
			resp, err := http.Get(serverURL + "/api/v1/workflows/" + wfID + "/versions")
			if err != nil {
				fmt.Printf("Connection error: %v\n", err)
				os.Exit(1)
			}
			defer resp.Body.Close()

			var res struct {
				Versions []map[string]interface{} `json:"versions"`
			}
			_ = json.NewDecoder(resp.Body).Decode(&res)

			w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
			fmt.Fprintln(w, "VERSION\tSTATUS\tCREATED AT")
			for _, v := range res.Versions {
				fmt.Fprintf(w, "v%v\t%v\t%v\n", v["version_num"], v["status"], v["created_at"])
			}
			w.Flush()
		},
	}
	rootCmd.AddCommand(versionsCmd)

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
