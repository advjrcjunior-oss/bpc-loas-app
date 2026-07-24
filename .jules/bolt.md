## 2024-10-24 - Pre-fork Connection Pooling
**Learning:** In a pre-fork server environment like Gunicorn, globally scoped network state (like `requests.Session()`) is not fork-safe and can cause connection issues across workers.
**Action:** Lazily initialize connection pools via a helper function (e.g., `get_session()`) to defer creation until after the worker process has forked.
