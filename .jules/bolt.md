## 2024-05-30 - Memory-inefficient file duplicate detection
**Learning:** The `detect_duplicates` function reads entire files into memory to hash them, which causes severe memory pressure and slow execution for large legal PDF/image batches. It also computes hashes for all files unconditionally.
**Action:** Always pre-filter by file size when checking for duplicates, and read large files in chunks when generating hashes to avoid loading the entire file into RAM.
