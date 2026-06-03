## 2024-06-03 - Initial Setup\n**Learning:** Started exploring repo\n**Action:** None yet

## 2024-06-03 - Optimize `detect_duplicates`
**Learning:** Checking file size before hashing files avoids unnecessary and expensive file reads and MD5 hashing, especially for large batches of PDFs and images. When reading files to compute their hash, using `fh.read()` reads the entire file into memory which can cause large memory spikes for large PDFs.
**Action:** Use chunked reading (`fh.read(65536)`) and pre-filter by file size when iterating through and hashing large batches of files.
