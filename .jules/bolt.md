## 2024-06-12 - [Do not use lru_cache for CPFCNPJ API functions]
**Learning:** Downstream functions (like `validar_dados_cliente`) mutate the dictionary returned by API functions (`consultar_cpf`, `consultar_cnpj`). Using `@functools.lru_cache` directly on these functions causes cache corruption and severe issues.
**Action:** Rely on `requests.Session()` for connection pooling instead of caching the parsed dicts, or use deepcopy if caching is absolutely necessary.
