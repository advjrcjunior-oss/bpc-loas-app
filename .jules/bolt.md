## 2024-05-23 - Avoid reading whole large PDFs into memory
**Learning:** Found an anti-pattern in `detect_duplicates` where large PDF files were read entirely into memory (`fh.read()`) and hashed unconditionally to find duplicates. This caused high memory usage and slow performance during batch processing.
**Action:** Always pre-filter files by size (`os.path.getsize()`) before hashing. Only hash files that share the same size, and use chunked reading (`fh.read(65536)`) when calculating the hash to keep memory usage low.
