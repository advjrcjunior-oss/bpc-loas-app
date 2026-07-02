## 2026-03-25 - Connection Pooling for External APIs
**Learning:** External API lookups (like ViaCEP, cpfcnpj.com.br) using plain `requests.get()` inside loops cause significant N+1 overhead due to repeated TLS handshakes during batch processing.
**Action:** Always use a shared `requests.Session()` to enable TCP keep-alive and connection pooling when interacting with external REST APIs during batch operations.
