## 2024-07-25 - Gunicorn Fork Safety with Connection Pooling
**Learning:** In a pre-fork server environment like Gunicorn (used by this app), globally scoped network state like `requests.Session()` is not fork-safe. If initialized at the module level before forking, multiple worker processes will share the same socket state, leading to errors or corrupted connections.
**Action:** Always lazily initialize connection pooling objects (e.g., using a `get_session()` helper function) so they are created after the worker process has forked, on first use.
