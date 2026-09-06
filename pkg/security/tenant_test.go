package security_test

import (
	"context"
	"testing"

	"github.com/Lab-OpenFlow/openflow/pkg/security"
)

func TestTenantContext(t *testing.T) {
	ctx := context.Background()

	// Default fallback
	if tenant := security.GetTenant(ctx); tenant != security.DefaultTenant {
		t.Fatalf("expected %s, got %s", security.DefaultTenant, tenant)
	}

	// Custom tenant
	ctxWithTenant := security.WithTenant(ctx, "acme-corp")
	if tenant := security.GetTenant(ctxWithTenant); tenant != "acme-corp" {
		t.Fatalf("expected acme-corp, got %s", tenant)
	}

	// Empty string fallback
	ctxEmpty := security.WithTenant(ctx, "")
	if tenant := security.GetTenant(ctxEmpty); tenant != security.DefaultTenant {
		t.Fatalf("expected %s, got %s", security.DefaultTenant, tenant)
	}
}
