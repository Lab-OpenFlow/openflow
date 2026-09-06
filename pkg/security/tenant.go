package security

import "context"

type tenantContextKey struct{}

const DefaultTenant = "default"

// WithTenant attaches a tenant identifier to the context.
func WithTenant(ctx context.Context, tenantID string) context.Context {
	if tenantID == "" {
		tenantID = DefaultTenant
	}
	return context.WithValue(ctx, tenantContextKey{}, tenantID)
}

// GetTenant retrieves the tenant identifier from the context, defaulting to "default".
func GetTenant(ctx context.Context) string {
	if ctx == nil {
		return DefaultTenant
	}
	if val, ok := ctx.Value(tenantContextKey{}).(string); ok && val != "" {
		return val
	}
	return DefaultTenant
}
