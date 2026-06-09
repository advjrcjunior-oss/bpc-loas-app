## 2024-05-18 - [Add HTTP connection pooling via requests.Session]
**Learning:** Using `requests.get` repeatedly without a session causes N+1 connection overhead for external API calls, hurting performance.
**Action:** Use `requests.Session()` to enable TCP connection pooling for API clients.
