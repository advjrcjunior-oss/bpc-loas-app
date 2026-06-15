## 2026-06-15 - Connection Pooling in REST API Integrations
**Learning:** Creating new HTTP connections per request via `requests.get()` in high-volume services (like `cpfcnpj_service`) causes significant TLS handshake overhead.
**Action:** Use a global `requests.Session()` to enable TCP connection pooling for recurring API calls to the same host, avoiding `lru_cache` if response dictionaries are mutated downstream.
