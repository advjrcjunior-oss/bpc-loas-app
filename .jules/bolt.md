## 2026-07-21 - [TLS Handshake Overhead in External REST APIs]
**Learning:** Using `requests.get` directly on external APIs like ViaCEP or CPF/CNPJ incurs a heavy TLS negotiation penalty on every call during batch workflows.
**Action:** Always use a global `requests.Session()` object when performing multiple requests to the same external host to leverage TCP keep-alive and connection pooling.
