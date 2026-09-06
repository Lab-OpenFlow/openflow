# Security Policy

## Supported Versions

| Version | Supported |
|---------|-----------|
| 0.1.x   | ✅ Yes     |

## Reporting a Vulnerability

**Please do not report security vulnerabilities through public GitHub issues.**

To report a security vulnerability, email the maintainers at the address listed in the GitHub organization profile, or open a [GitHub Security Advisory](https://github.com/Lab-OpenFlow/openflow/security/advisories/new).

Please include:
- Description of the vulnerability.
- Steps to reproduce.
- Potential impact.
- Suggested fix (if any).

You should receive a response within 72 hours. We will coordinate a fix and disclosure timeline with you.

## Security Configuration

OpenFlow is designed to be secure by default, but requires proper configuration for production deployments:

### Required Environment Variables

| Variable | Description | Required in Production |
|---|---|---|
| `OPENFLOW_JWT_SECRET` | Secret key for signing JWT tokens (min 32 chars) | **Yes** |
| `OPENFLOW_ADMIN_PASSWORD` | Password for the built-in `admin` user | **Yes** |
| `OPENFLOW_OPERATOR_PASSWORD` | Password for the built-in `operator` user | **Yes** |
| `OPENFLOW_AUDITOR_PASSWORD` | Password for the built-in `auditor` user | **Yes** |
| `OPENFLOW_VAULT_KEY` | AES-GCM 256-bit encryption key for the secret vault | **Yes** |

If any of these are not set, OpenFlow will generate a random value at startup and log a warning. **Random values will not persist across restarts.**

### Recommendations

- Run behind a TLS-terminating reverse proxy (NGINX, Caddy, AWS ALB).
- Restrict the Prometheus `/metrics` and `/audits/export` endpoints to internal networks.
- Rotate API keys regularly using the `openflowctl` CLI.
- Enable PostgreSQL SSL (`sslmode=require` in your DSN).
