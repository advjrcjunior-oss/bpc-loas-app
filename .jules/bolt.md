## 2024-05-24 - Connection Pooling vs LRU Cache for API Results
**Learning:** When interacting with external APIs (like cpfcnpj.com.br) where downstream functions mutate the returned dictionaries, using @functools.lru_cache on the API call directly corrupts the cache. Using requests.Session() provides connection pooling for TCP keep-alive, safely improving performance during batch operations without side effects.
**Action:** Always prefer requests.Session() for connection pooling over caching mutable API responses.
