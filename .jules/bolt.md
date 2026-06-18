## 2024-06-18 - Optimized REST API connection pooling
**Learning:** In batch processing workflows, avoiding TCP handshake overhead for every single request using `requests.Session()` is an effective strategy for APIs like ViaCEP and CPFCNPJ.
**Action:** Use connection pooling for REST API calls and wrap stateless lookup queries with LRU cache if dictionaries are not mutated downstream (e.g. `validar_cep` and `buscar_cep_por_endereco`).
## 2024-06-18 - Caching API dict responses
**Learning:** Using `@functools.lru_cache` on functions that return dictionaries parsed from API responses can lead to cache pollution if downstream code mutates the dictionary. Also, caching functions that return `None` on transient network errors permanently caches the failure.
**Action:** Do not use `lru_cache` directly on API functions returning mutable structures or capturing temporary errors. Rely primarily on `requests.Session()` connection pooling.
