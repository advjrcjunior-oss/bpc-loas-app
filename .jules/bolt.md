## 2025-01-20 - Connection Pooling for API Requests
**Learning:** During batch processing, using `@functools.lru_cache` on external API requests (like cpfcnpj_service) can cause cache corruption if the returned dictionaries are mutated downstream. Additionally, creating new HTTP requests sequentially for every call creates significant network overhead.
**Action:** Use `requests.Session()` at the module level for TCP connection pooling (keep-alive). This provides significant performance benefits during batch processing without the risk of mutating cached objects across separate requests.
