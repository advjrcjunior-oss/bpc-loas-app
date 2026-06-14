## 2024-05-24 - Initial Bolt Journal
## 2025-02-12 - External API Caching & Connection Pooling Gotchas
**Learning:** External lookup requests (ViaCEP, cpfcnpj) can bottleneck batch processing heavily due to N+1 lookups. Using `requests.Session()` is a standard optimization for TCP keep-alive (connection pooling). However, applying `@functools.lru_cache` to `cpfcnpj_service.py` is dangerous and causes cache corruption because the returned dictionaries are mutated downstream (e.g. `validar_dados_cliente`).
**Action:** Always check if returned data from API wrappers is mutated downstream before applying `@functools.lru_cache`. For cases where dictionaries are mutated, stick to `requests.Session()` for connection pooling to optimize network latency without introducing mutable cache bugs.
