## 2024-05-24 - API Connection Pooling in Batch Jobs
**Learning:** External REST APIs (ViaCEP, cpfcnpj) were being called repeatedly using `requests.get()`, which creates and tears down a TCP connection for every single call. In batch processing flows (`processar_lote_v2.py`), this lack of connection pooling introduced significant overhead and latency.
**Action:** Always use `requests.Session()` to enable TCP keep-alive for external APIs that are called frequently in batch operations or loops. This simple change reduces the connection overhead by reusing existing sockets.
