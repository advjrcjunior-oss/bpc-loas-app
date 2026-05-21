## 2024-05-24 - File Hashing Bottleneck in Batch Processing
**Learning:** Hashing the entire contents of every file to find duplicates is a massive performance bottleneck when dealing with folders full of large documents (PDFs, images) typical in this application's batch processing workflow.
**Action:** Always pre-filter by file size before reading file contents. Two files can only be identical if their byte sizes are exactly the same. Only calculate MD5 hashes for files that share a size with at least one other file.
