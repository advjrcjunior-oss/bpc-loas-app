## 2025-05-18 - Use Connection Pooling for External REST APIs
**Learning:** During batch processing workflows (like document uploads or validations), making synchronous HTTP requests to external APIs (like ViaCEP or cpfcnpj.com.br) using `requests.get()` without a session creates a significant bottleneck due to the overhead of repeated TCP connections and TLS handshakes.
**Action:** Always use `requests.Session()` to enable connection pooling and TCP keep-alive when calling external REST APIs, especially inside loops or batch operations.
