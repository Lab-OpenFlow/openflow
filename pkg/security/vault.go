package security

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
)

// Vault provides AES-GCM 256-bit encryption and decryption for sensitive secrets and API tokens.
type Vault struct {
	key []byte
}

// NewVault creates a new Secret Vault using a 32-byte encryption key.
func NewVault(secretKey string) (*Vault, error) {
	if len(secretKey) == 0 {
		secretKey = uuid.New().String() + uuid.New().String() // 72-char random key
		slog.Warn("OPENFLOW_VAULT_KEY is not set; using a random encryption key. Secrets will not persist across restarts. Set OPENFLOW_VAULT_KEY in production.")
	}
	keyBytes := []byte(secretKey)
	if len(keyBytes) < 32 {
		padded := make([]byte, 32)
		copy(padded, keyBytes)
		keyBytes = padded
	} else if len(keyBytes) > 32 {
		keyBytes = keyBytes[:32]
	}

	return &Vault{key: keyBytes}, nil
}

// Encrypt encrypts plain text into a base64 encoded AES-GCM ciphertext.
func (v *Vault) Encrypt(plaintext string) (string, error) {
	block, err := aes.NewCipher(v.key)
	if err != nil {
		return "", err
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}

	nonce := make([]byte, gcm.NonceSize())
	if _, err = io.ReadFull(rand.Reader, nonce); err != nil {
		return "", err
	}

	ciphertext := gcm.Seal(nonce, nonce, []byte(plaintext), nil)
	return base64.StdEncoding.EncodeToString(ciphertext), nil
}

// Decrypt decrypts a base64 encoded AES-GCM ciphertext back into plain text.
func (v *Vault) Decrypt(cipherTextBase64 string) (string, error) {
	data, err := base64.StdEncoding.DecodeString(cipherTextBase64)
	if err != nil {
		return "", err
	}

	block, err := aes.NewCipher(v.key)
	if err != nil {
		return "", err
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}

	nonceSize := gcm.NonceSize()
	if len(data) < nonceSize {
		return "", errors.New("ciphertext too short")
	}

	nonce, ciphertext := data[:nonceSize], data[nonceSize:]
	plaintext, err := gcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return "", fmt.Errorf("decryption failed: %w", err)
	}

	return string(plaintext), nil
}

// SecretEntry represents metadata for a stored secret.
type SecretEntry struct {
	Key       string    `json:"key"`
	Encrypted string    `json:"-"`
	UpdatedAt time.Time `json:"updated_at"`
}

// SecretVaultManager manages encrypted secrets with PostgreSQL persistence.
type SecretVaultManager struct {
	mu      sync.RWMutex
	vault   *Vault
	db      *sql.DB
	secrets map[string]SecretEntry
}

var GlobalVault *SecretVaultManager

func init() {
	v, _ := NewVault("")
	GlobalVault = &SecretVaultManager{
		vault: v,
		secrets: map[string]SecretEntry{
			"service/token": {
				Key:       "service/token",
				UpdatedAt: time.Now(),
			},
			"database/dsn": {
				Key:       "database/dsn",
				UpdatedAt: time.Now(),
			},
			"provider/api_key": {
				Key:       "provider/api_key",
				UpdatedAt: time.Now(),
			},
		},
	}
	_ = GlobalVault.SetSecret("service/token", "tok_live_service_default")
	_ = GlobalVault.SetSecret("database/dsn", "postgres://app:secure_pass@localhost:5432/main")
	_ = GlobalVault.SetSecret("provider/api_key", "api_key_default_provider")
}

// AttachDB binds PostgreSQL database and loads persistent secrets into the decrypted cache.
func (m *SecretVaultManager) AttachDB(db *sql.DB) error {
	m.mu.Lock()
	m.db = db
	m.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Ensure table exists
	_, err := db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS openflow_vault_secrets (
			key VARCHAR(255) PRIMARY KEY,
			encrypted_value TEXT NOT NULL,
			updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		);
	`)
	if err != nil {
		return fmt.Errorf("failed to migrate openflow_vault_secrets: %w", err)
	}

	// Load existing persistent secrets from PostgreSQL into memory cache
	rows, err := db.QueryContext(ctx, "SELECT key, encrypted_value, updated_at FROM openflow_vault_secrets")
	if err != nil {
		return err
	}
	defer rows.Close()

	m.mu.Lock()
	defer m.mu.Unlock()
	for rows.Next() {
		var entry SecretEntry
		if err := rows.Scan(&entry.Key, &entry.Encrypted, &entry.UpdatedAt); err == nil {
			m.secrets[entry.Key] = entry
		}
	}

	slog.Info("synchronized secrets with storage", slog.Int("count", len(m.secrets)))
	return nil
}

// SetSecret encrypts and stores a secret in memory and PostgreSQL.
func (m *SecretVaultManager) SetSecret(key, plaintext string) error {
	enc, err := m.vault.Encrypt(plaintext)
	if err != nil {
		return err
	}

	now := time.Now()
	m.mu.Lock()
	m.secrets[key] = SecretEntry{
		Key:       key,
		Encrypted: enc,
		UpdatedAt: now,
	}
	db := m.db
	m.mu.Unlock()

	if db != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		query := `
			INSERT INTO openflow_vault_secrets (key, encrypted_value, updated_at)
			VALUES ($1, $2, $3)
			ON CONFLICT (key) DO UPDATE SET
				encrypted_value = EXCLUDED.encrypted_value,
				updated_at = EXCLUDED.updated_at
		`
		_, _ = db.ExecContext(ctx, query, key, enc, now)
	}

	return nil
}

// GetSecret decrypts and returns a secret plaintext.
func (m *SecretVaultManager) GetSecret(key string) (string, error) {
	m.mu.RLock()
	entry, exists := m.secrets[key]
	m.mu.RUnlock()

	if !exists {
		return "", fmt.Errorf("secret '%s' not found in vault", key)
	}

	return m.vault.Decrypt(entry.Encrypted)
}

// ListKeys returns metadata for all registered secrets.
func (m *SecretVaultManager) ListKeys() []SecretEntry {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make([]SecretEntry, 0, len(m.secrets))
	for _, entry := range m.secrets {
		result = append(result, entry)
	}
	return result
}

// DeleteSecret removes a secret from memory cache and PostgreSQL.
func (m *SecretVaultManager) DeleteSecret(key string) error {
	m.mu.Lock()
	if _, exists := m.secrets[key]; !exists {
		m.mu.Unlock()
		return errors.New("secret not found")
	}
	delete(m.secrets, key)
	db := m.db
	m.mu.Unlock()

	if db != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_, _ = db.ExecContext(ctx, "DELETE FROM openflow_vault_secrets WHERE key = $1", key)
	}

	return nil
}
