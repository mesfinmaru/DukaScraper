# Worker assignment

The worker router uses a nine-signal priority chain defined in [app/common/constants/worker_assignment.py](../../app/common/constants/worker_assignment.py):

1. User override
2. Dark-web TLDs such as `.onion` and `.i2p`
3. Known WAF/CDN-protected domains such as `cloudflare.com`
4. Deep-domain allowlist such as `linkedin.com`
5. Ethiopian government and telecom domains such as `ethiopia.gov.et`
6. URL path heuristics such as `/login` or `/auth`
7. Query parameter heuristics such as `token` or `oauth`
8. Ethiopian surface-safe domains such as `ena.et`
9. Default fallback to surface

Escalation triggers are also explicit: HTTP 401/403/407/429/451, auth forms, empty HTML shells, and JS-rendered placeholders.

Example signals:
- `https://www.linkedin.com/in/example` → deep via `deep_domain_whitelist`
- `https://ethiopia.gov.et` → deep via `ethiopian_gov_telecom_domain`
- `https://www.bbc.com/amharic` → surface via `default_fallback`
- `http://example.onion` → dark via `dark_web_tld`
