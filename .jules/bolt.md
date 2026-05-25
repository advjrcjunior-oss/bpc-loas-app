
## 2024-05-18 - Avoid loading large files into memory for batch processing
**Learning:** In batch workflows dealing with large files like PDFs or scanned images, using `read()` to load the entire file at once to compute a hash can cause high memory usage and slowdowns. Also, calculating hashes for all files in a folder unconditionally uses unnecessary CPU cycles.
**Action:** Always pre-filter files by size first to identify potential duplicates, and then hash files using chunked reading (e.g. `iter(lambda: fh.read(8192), b"")`) instead of loading everything in memory.
