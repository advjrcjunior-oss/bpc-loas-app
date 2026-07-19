## 2026-07-19 - Do not use lru_cache on CPF/CNPJ API
**Learning:** Do not use `@functools.lru_cache` directly on API functions like `consultar_cpf` or `consultar_cnpj` in `cpfcnpj_service.py` because the returned dictionaries are mutated by downstream functions (e.g., `validar_dados_cliente`), which causes cache corruption.
**Action:** Rely on `requests.Session()` for connection pooling instead.
