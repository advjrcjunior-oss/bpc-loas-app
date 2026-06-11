
## 2024-06-11 - [LRU Cache Mutation Bug]
**Learning:** [When applying `@functools.lru_cache` to functions returning mutable objects (like dicts from `r.json()`), downstream code might modify the cached instance, leading to cross-request bugs.]
**Action:** [Always cache the raw data or return a `dict(cached_data)` copy if the returned data structure is mutable and expected to be safely handled.]
