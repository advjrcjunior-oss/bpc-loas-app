## 2026-07-05 - Connection Pooling & Cache Corruption
**Learning:** Using `@functools.lru_cache` on functions returning dictionaries from external APIs (like `consultar_cpf`) causes cache corruption if downstream functions mutate the returned dictionary. Also, creating a new `requests.get` session for every API call in batch workflows (e.g. CPFCNPJ, ViaCEP) causes severe TCP exhaustion/bottlenecks.
**Action:** Rely on `requests.Session()` for connection pooling to speed up external API requests instead of using `@functools.lru_cache` on dictionary-returning functions.
