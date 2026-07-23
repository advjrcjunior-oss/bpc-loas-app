## 2024-07-23 - Requests Session in Pre-Fork Servers
**Learning:** Initializing `requests.Session()` at module level is not safe in pre-fork architectures (like Gunicorn/uWSGI) because file descriptors for open sockets can be inherited by child processes, leading to broken pipes or `BadStatusLine` errors.
**Action:** Always instantiate connection pools lazily (e.g. `if session is None: session = requests.Session()`) inside the function where it will be used, rather than relying on module-level setup.
