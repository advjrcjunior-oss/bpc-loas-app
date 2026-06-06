## 2024-05-24 - N+1 Queries Multiplied by Rate Limits
**Learning:** In systems with mandatory API rate limiting (like LegalMail's 4s delay), an N+1 query bug transforms from a minor slowdown into a severe bottleneck. The `upload_todos_anexos` process was taking 40+ seconds for 10 files because `get_tipos_anexo` queried the API un-cached per file, triggering the rate limiter each time.
**Action:** Aggressively memoize/cache small dictionary or type-lookup API endpoints that are accessed inside loops, especially when the underlying HTTP client enforces a rate-limit delay.
