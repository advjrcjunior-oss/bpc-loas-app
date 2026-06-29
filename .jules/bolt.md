## 2024-05-18 - Connection Pooling for External APIs
**Learning:** External API lookups (ViaCEP, CPFCNPJ) used in batch workflows severely bottleneck performance when not using connection pooling, due to repeated TCP/TLS handshakes. Using `functools.lru_cache` on these exception-catching API functions is dangerous as it might permanently cache transient errors.
**Action:** Always use `requests.Session()` to enable HTTP Keep-Alive for external APIs instead of bare `requests.get/post`, especially for batch processing where the same API is called repeatedly.
