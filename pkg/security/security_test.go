package security_test

import (
	"testing"
	"time"

	"github.com/Lab-OpenFlow/openflow/pkg/security"
)

func TestVaultEncryptDecrypt(t *testing.T) {
	vault, err := security.NewVault("test-secret-vault-key-32bytes!")
	if err != nil {
		t.Fatalf("failed to create vault: %v", err)
	}

	secret := "postgres://user:super_secret_password@db.prod.internal:5432/main"
	encrypted, err := vault.Encrypt(secret)
	if err != nil {
		t.Fatalf("encryption failed: %v", err)
	}

	if encrypted == secret {
		t.Fatalf("expected encrypted text to differ from plaintext")
	}

	decrypted, err := vault.Decrypt(encrypted)
	if err != nil {
		t.Fatalf("decryption failed: %v", err)
	}

	if decrypted != secret {
		t.Fatalf("decrypted text does not match original plaintext. Got %s, expected %s", decrypted, secret)
	}
}

func TestJWTTokenGenerationAndValidation(t *testing.T) {
	auth := security.NewAuthManager("test-jwt-secret-key-123")

	token, err := auth.GenerateToken("user-123", security.RoleAdmin, 1*time.Hour)
	if err != nil {
		t.Fatalf("token generation failed: %v", err)
	}

	claims, err := auth.ValidateToken(token)
	if err != nil {
		t.Fatalf("token validation failed: %v", err)
	}

	if claims.Subject != "user-123" {
		t.Fatalf("expected subject 'user-123', got '%s'", claims.Subject)
	}
	if claims.Role != security.RoleAdmin {
		t.Fatalf("expected role 'admin', got '%s'", claims.Role)
	}
}
