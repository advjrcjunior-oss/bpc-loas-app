## 2024-05-18 - Avoid reading entire files in memory when hashing
**Learning:** The `detect_duplicates` function was reading the entire content of potentially large files (PDFs, images) into memory at once just to calculate an MD5 hash using `fh.read()`.
**Action:** Always read files in chunks when hashing or performing operations that do not require the whole file in memory at once. I used a chunk size of 8192 bytes and `hashlib.md5().update(chunk)`.
