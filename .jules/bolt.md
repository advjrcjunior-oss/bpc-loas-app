## 2026-06-21 - External API Connection Pooling
**Learning:** For third party APIs used repeatedly across batch process workflows (ViaCEP, cpfcnpj), direct `requests.get()` usage creates huge overhead due to fresh SSL/TCP handshakes per request.
**Action:** Always instantiate `requests.Session()` to use HTTP connection pooling/TCP keep-alive.
