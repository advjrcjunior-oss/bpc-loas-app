## 2024-07-08 - Session caching external API connection
**Learning:** Using `requests.get` multiple times creates new TCP connections. Reusing `requests.Session()` is faster because it pools connections using TCP Keep-Alive.
**Action:** Replace `requests.get` with `requests.Session()` in places where we query external APIs often.
