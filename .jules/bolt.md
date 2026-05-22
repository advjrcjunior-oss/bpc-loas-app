
## 2025-05-14 - Pre-filtering by File Size and Chunking I/O for Large Documents
**Learning:** Reading large PDFs and images directly into memory using `fh.read()` causes high memory usage and potential bottlenecks, especially when calculating MD5 hashes to detect duplicates. In batch processing applications like this, files should be compared by their size first to avoid expensive hashing for unique files.
**Action:** When working with large files, always pre-filter collections by file size to avoid processing every file. When hashing or processing is necessary, read files in chunks (e.g., `8192` bytes) instead of loading the entire content into memory at once.
