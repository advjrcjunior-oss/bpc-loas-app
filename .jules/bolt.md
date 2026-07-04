## 2024-05-18 - Caching API responses that get mutated
**Learning:** Using `@functools.lru_cache` directly on API functions like `consultar_cpf` or `consultar_cnpj` in `cpfcnpj_service.py` is an anti-pattern. The returned dictionaries are mutated by downstream functions (e.g., `validar_dados_cliente`), which causes cache corruption.
**Action:** Rely on `requests.Session()` for connection pooling instead of caching mutable return values.
