## 2024-05-24 - API Connection Pooling in Batch Processing
**Learning:** Repeatedly calling external APIs (like ViaCEP or CPF/CNPJ) during batch processing workflows without connection pooling (e.g. using bare `requests.get`) incurs severe TCP handshake overhead, creating a bottleneck.
**Action:** Always instantiate `requests.Session()` to keep connections alive and reuse them across requests instead of direct `requests.get`.
