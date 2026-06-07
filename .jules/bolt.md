## 2024-06-07 - LegalMail API Rate Limit Bottleneck
**Learning:** The external LegalMail API enforces a strict rate limit delay (e.g., a 4-second minimum delay between requests). This turns even small N+1 API calls, such as repeatedly looking up document attachment types during a batch upload, into severe performance bottlenecks.
**Action:** Always verify if an external API call inside a loop can be aggressively cached per-item or per-batch (e.g., using an instance-level `_options_cache` dictionary) to prevent N+1 performance degradations.
