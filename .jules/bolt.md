## 2026-03-25 - [Use requests.Session() for API connection pooling]
**Learning:** [Using `@functools.lru_cache` on API functions returning dictionaries can lead to cache corruption due to downstream mutation, or permanently caching transient network failures. Using `requests.Session()` provides safe performance benefits via TCP keep-alive connection pooling during batch operations.]
**Action:** [Always use `requests.Session()` for repeated external API calls instead of `@functools.lru_cache` when responses are mutated or can represent transient failures.]
