## 2024-05-23 - Avoid LRU Cache on external API responses returned as mutable dicts
**Learning:** Do not use `@functools.lru_cache` directly on API functions like `consultar_cpf` or `consultar_cnpj` in `cpfcnpj_service.py`. The returned dictionaries are mutated by downstream functions (e.g., `validar_dados_cliente`), which causes cache corruption where subsequent requests get modified data.
**Action:** Rely on `requests.Session()` for connection pooling to optimize the network layer instead of blindly caching at the application layer, avoiding unintended state mutations.
