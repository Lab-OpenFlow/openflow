package security

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
)

// Role defines the authorization level.
type Role string

const (
	RoleAdmin    Role = "admin"
	RoleOperator Role = "operator"
	RoleViewer   Role = "viewer"
)

// UserAccount represents a registered user with salted password hashing.
type UserAccount struct {
	Username     string `json:"username"`
	PasswordHash string `json:"-"`
	Salt         string `json:"-"`
	FullName     string `json:"full_name"`
	Role         Role   `json:"role"`
}

// APIKeyMetadata represents the secure metadata of a registered API key.
type APIKeyMetadata struct {
	ID         string     `json:"id"`
	Name       string     `json:"name"`
	Role       Role       `json:"role"`
	KeyPrefix  string     `json:"key_prefix"` // e.g. "ofk_live_admin_a9f8...12c4"
	KeyHash    string     `json:"-"`          // SHA-256 one-way hash
	CreatedAt  time.Time  `json:"created_at"`
	LastUsedAt *time.Time `json:"last_used_at,omitempty"`
	ExpiresAt  *time.Time `json:"expires_at,omitempty"`
}

// Claims represents JWT claims.
type Claims struct {
	Subject  string `json:"sub"`
	Role     Role   `json:"role"`
	FullName string `json:"name"`
	jwt.RegisteredClaims
}

// AuthManager handles token creation, validation, user login, RBAC and CSPRNG API Key management.
type AuthManager struct {
	mu        sync.RWMutex
	jwtSecret []byte
	apiKeys   map[string]*APIKeyMetadata // keyed by ID
	hashIndex map[string]string          // KeyHash -> ID
	users     map[string]UserAccount
}

// hashPassword returns a salt and salted SHA-256 hash.
func hashPassword(password string, saltHex string) (string, string) {
	if saltHex == "" {
		saltBytes := make([]byte, 16)
		_, _ = rand.Read(saltBytes)
		saltHex = hex.EncodeToString(saltBytes)
	}
	combined := append([]byte(password), []byte(saltHex)...)
	hash := sha256.Sum256(combined)
	return hex.EncodeToString(hash[:]), saltHex
}

// verifyPassword checks a candidate password against stored salt and hash in constant time.
func verifyPassword(password, saltHex, expectedHashHex string) bool {
	candidateHash, _ := hashPassword(password, saltHex)
	return subtle.ConstantTimeCompare([]byte(candidateHash), []byte(expectedHashHex)) == 1
}

// NewAuthManager creates a new AuthManager with environment-driven credentials.
func NewAuthManager(secret string) *AuthManager {
	if secret == "" {
		if envSecret := os.Getenv("OPENFLOW_JWT_SECRET"); envSecret != "" {
			secret = envSecret
		} else {
			secret = "openflow-jwt-enterprise-secret-" + uuid.New().String()
		}
	}

	adminPass := os.Getenv("OPENFLOW_ADMIN_PASSWORD")
	if adminPass == "" {
		adminPass = uuid.New().String()
		slog.Warn("OPENFLOW_ADMIN_PASSWORD is not set; using a random password. Set this environment variable in production.")
	}

	operatorPass := os.Getenv("OPENFLOW_OPERATOR_PASSWORD")
	if operatorPass == "" {
		operatorPass = uuid.New().String()
		slog.Warn("OPENFLOW_OPERATOR_PASSWORD is not set; using a random password. Set this environment variable in production.")
	}

	auditorPass := os.Getenv("OPENFLOW_AUDITOR_PASSWORD")
	if auditorPass == "" {
		auditorPass = uuid.New().String()
		slog.Warn("OPENFLOW_AUDITOR_PASSWORD is not set; using a random password. Set this environment variable in production.")
	}

	adminHash, adminSalt := hashPassword(adminPass, "")
	opHash, opSalt := hashPassword(operatorPass, "")
	auditHash, auditSalt := hashPassword(auditorPass, "")

	mgr := &AuthManager{
		jwtSecret: []byte(secret),
		apiKeys:   make(map[string]*APIKeyMetadata),
		hashIndex: make(map[string]string),
		users: map[string]UserAccount{
			"admin": {
				Username:     "admin",
				PasswordHash: adminHash,
				Salt:         adminSalt,
				FullName:     "System Administrator",
				Role:         RoleAdmin,
			},
			"operator": {
				Username:     "operator",
				PasswordHash: opHash,
				Salt:         opSalt,
				FullName:     "Operations Operator",
				Role:         RoleOperator,
			},
			"auditor": {
				Username:     "auditor",
				PasswordHash: auditHash,
				Salt:         auditSalt,
				FullName:     "Security Auditor",
				Role:         RoleViewer,
			},
		},
	}

	_, _, _ = mgr.GenerateAPIKey("Integration Service", RoleAdmin, 0)
	_, _, _ = mgr.GenerateAPIKey("Metrics Exporter Worker", RoleOperator, 0)

	return mgr
}

// AuthenticateUser verifies username and password with constant-time hash verification.
func (a *AuthManager) AuthenticateUser(username, password string) (*UserAccount, string, error) {
	a.mu.RLock()
	user, exists := a.users[username]
	a.mu.RUnlock()

	if !exists || !verifyPassword(password, user.Salt, user.PasswordHash) {
		return nil, "", errors.New("invalid credentials")
	}

	token, err := a.GenerateToken(user.Username, user.Role, 24*time.Hour)
	if err != nil {
		return nil, "", err
	}

	return &user, token, nil
}

// ListUsers returns all registered users without passwords.
func (a *AuthManager) ListUsers() []UserAccount {
	a.mu.RLock()
	defer a.mu.RUnlock()

	result := make([]UserAccount, 0, len(a.users))
	for _, u := range a.users {
		result = append(result, UserAccount{
			Username: u.Username,
			FullName: u.FullName,
			Role:     u.Role,
		})
	}
	return result
}

// CreateUser registers or updates a user account with salted SHA-256 hash.
func (a *AuthManager) CreateUser(username, password, fullName string, role Role) error {
	if username == "" || password == "" {
		return errors.New("username and password are required")
	}
	if role == "" {
		role = RoleViewer
	}

	passHash, salt := hashPassword(password, "")
	u := UserAccount{
		Username:     username,
		PasswordHash: passHash,
		Salt:         salt,
		FullName:     fullName,
		Role:         role,
	}

	a.mu.Lock()
	defer a.mu.Unlock()
	a.users[u.Username] = u
	return nil
}

// DeleteUser removes a user account. Root admin cannot be removed.
func (a *AuthManager) DeleteUser(username string) error {
	if username == "admin" {
		return errors.New("cannot delete root administrator")
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	if _, exists := a.users[username]; !exists {
		return errors.New("user not found")
	}

	delete(a.users, username)
	return nil
}

// GenerateAPIKey generates a cryptographically secure API key using 256 bits of CSPRNG entropy.
func (a *AuthManager) GenerateAPIKey(name string, role Role, expiresInDays int) (*APIKeyMetadata, string, error) {
	if name == "" {
		name = "Service Integration Key"
	}
	if role == "" {
		role = RoleOperator
	}

	entropy := make([]byte, 32)
	if _, err := rand.Read(entropy); err != nil {
		return nil, "", fmt.Errorf("failed to generate secure entropy: %w", err)
	}

	rawEntropyHex := hex.EncodeToString(entropy)
	rawKey := fmt.Sprintf("ofk_live_%s_%s", role, rawEntropyHex)

	hashBytes := sha256.Sum256([]byte(rawKey))
	keyHash := hex.EncodeToString(hashBytes[:])

	prefix := fmt.Sprintf("%s...%s", rawKey[:18], rawKey[len(rawKey)-6:])
	keyID := "key_" + uuid.New().String()[:8]

	meta := &APIKeyMetadata{
		ID:        keyID,
		Name:      name,
		Role:      role,
		KeyPrefix: prefix,
		KeyHash:   keyHash,
		CreatedAt: time.Now(),
	}

	if expiresInDays > 0 {
		exp := time.Now().AddDate(0, 0, expiresInDays)
		meta.ExpiresAt = &exp
	}

	a.mu.Lock()
	defer a.mu.Unlock()
	a.apiKeys[meta.ID] = meta
	a.hashIndex[meta.KeyHash] = meta.ID

	return meta, rawKey, nil
}

// ListAPIKeys returns all registered API keys metadata (with masked prefix).
func (a *AuthManager) ListAPIKeys() []*APIKeyMetadata {
	a.mu.RLock()
	defer a.mu.RUnlock()

	result := make([]*APIKeyMetadata, 0, len(a.apiKeys))
	for _, meta := range a.apiKeys {
		result = append(result, meta)
	}
	return result
}

// ValidateAPIKey checks if a raw API key is valid using constant-time hash comparison.
func (a *AuthManager) ValidateAPIKey(rawKey string) (Role, bool) {
	if len(rawKey) < 10 {
		return "", false
	}

	hashBytes := sha256.Sum256([]byte(rawKey))
	candidateHash := hex.EncodeToString(hashBytes[:])

	a.mu.Lock()
	defer a.mu.Unlock()

	keyID, exists := a.hashIndex[candidateHash]
	if !exists {
		return "", false
	}

	meta, found := a.apiKeys[keyID]
	if !found {
		return "", false
	}

	if meta.ExpiresAt != nil && time.Now().After(*meta.ExpiresAt) {
		return "", false
	}

	if subtle.ConstantTimeCompare([]byte(meta.KeyHash), []byte(candidateHash)) != 1 {
		return "", false
	}

	now := time.Now()
	meta.LastUsedAt = &now

	return meta.Role, true
}

// RevokeAPIKey immediately revokes an API key by its ID.
func (a *AuthManager) RevokeAPIKey(id string) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	meta, exists := a.apiKeys[id]
	if !exists {
		return errors.New("api key not found")
	}

	delete(a.hashIndex, meta.KeyHash)
	delete(a.apiKeys, id)
	return nil
}

// GenerateToken generates a JWT token for a subject and role.
func (a *AuthManager) GenerateToken(subject string, role Role, duration time.Duration) (string, error) {
	a.mu.RLock()
	fullName := subject
	if u, ok := a.users[subject]; ok {
		fullName = u.FullName
	}
	a.mu.RUnlock()

	claims := &Claims{
		Subject:  subject,
		Role:     role,
		FullName: fullName,
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(duration)),
			IssuedAt:  jwt.NewNumericDate(time.Now()),
			Issuer:    "openflow",
		},
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return token.SignedString(a.jwtSecret)
}

// ValidateToken validates a JWT token string.
func (a *AuthManager) ValidateToken(tokenString string) (*Claims, error) {
	token, err := jwt.ParseWithClaims(tokenString, &Claims{}, func(token *jwt.Token) (interface{}, error) {
		return a.jwtSecret, nil
	})
	if err != nil {
		return nil, err
	}

	if claims, ok := token.Claims.(*Claims); ok && token.Valid {
		return claims, nil
	}

	return nil, errors.New("invalid jwt token")
}
