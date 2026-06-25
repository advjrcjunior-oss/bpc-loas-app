## 2024-06-25 - Avoid LRU Cache on external API lookups catching exceptions
**Learning:** Using `@functools.lru_cache` directly on API functions like `consultar_cpf` or `validar_cep` can permanently cache `None` due to transient network failures or cache corrupted mutable dictionaries when downstream functions modify them.
**Action:** Instead of caching API function returns directly to improve performance, utilize connection pooling via `requests.Session()` to reduce HTTP overhead while remaining robust to transient errors and mutation.
