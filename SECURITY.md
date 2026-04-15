# Security Policy

## Reporting a vulnerability

If you discover a security vulnerability, please report it responsibly.

**Do NOT open a public issue.** Instead, email: **security@dataspoc.com**

We will acknowledge receipt within 48 hours and provide an initial assessment within 7 days.

## Supported versions

| Version | Supported |
|---------|-----------|
| 0.1.x   | Yes       |

## Scope

DataSpoc Lens delegates all access control to cloud IAM. The following are in scope:

- SQL injection via user input
- Path traversal in export/cache operations
- Secret leakage in logs or AI prompts
- Dependency vulnerabilities

The following are out of scope:

- Cloud IAM misconfiguration (user responsibility)
- LLM provider vulnerabilities (report to provider)
