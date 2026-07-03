## 2024-05-19 - Connection Pooling for External REST APIs
**Learning:** For batch processing workflows making numerous external REST API calls (like ViaCEP or CPFCNPJ), creating new connections for every request causes severe network overhead.
**Action:** Always utilize `requests.Session()` to enable connection pooling and TCP keep-alive, significantly improving performance when repeatedly calling the same domain.
