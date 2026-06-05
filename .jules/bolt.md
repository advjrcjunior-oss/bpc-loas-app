## 2026-06-05 - Cache Attachment Types to Prevent N+1 Penalty
**Learning:** `get_tipos_anexo` makes a synchronous HTTP request for every file during the batch upload process (`upload_todos_anexos`). Due to the strict 4-second minimum delay between requests to the LegalMail API, this causes severe N+1 bottlenecks.
**Action:** Always aggressively cache read-only API lookup endpoints in batch loops, using mechanisms like `_options_cache`.
