# Security Policy

## Reporting a vulnerability

Please **do not** open a public GitHub issue for security vulnerabilities.

Use [GitHub private security advisories](https://github.com/gkoos/caracal/security/advisories/new) to report a vulnerability privately. We will acknowledge the report within 48 hours and aim to release a fix within 14 days for confirmed issues.

## Supported versions

| Version | Supported |
| ------- | --------- |
| 0.2.x   | ✅        |

Older versions are not supported once a new minor or major release is available.

## Scope

Vulnerabilities in the core policy engine, Redis coordination scripts, and bundled adapters are in scope. Issues in optional peer dependencies (`ioredis`, `pg`) should be reported to their respective maintainers unless the vulnerability is exploitable specifically through caracal's usage of those libraries.
