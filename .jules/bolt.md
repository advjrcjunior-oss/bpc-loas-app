## 2024-07-17 - Avoid caching mutable dicts in cpfcnpj_service
**Learning:** Do not use `@functools.lru_cache` directly on API functions like `consultar_cpf` or `consultar_cnpj` in `cpfcnpj_service.py` because the returned dictionaries are mutated by downstream functions (e.g., `validar_dados_cliente`), which causes cache corruption.
**Action:** Rely on `requests.Session()` for connection pooling instead of memoizing the parsed JSON dictionaries.
