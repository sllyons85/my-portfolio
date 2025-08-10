# IMDb Incremental ETL (xxhash64, low‑RAM, chunked)

A production‑style, portfolio‑ready ETL that ingests the official IMDb TSVs, computes an xxhash64 per row, and performs idempotent incremental upserts into MySQL. Designed for low RAM environments using chunked loads + LOAD DATA LOCAL INFILE for speed. First run builds the schema and (optionally) drops/rebuilds secondary indexes for faster bulk load; subsequent runs only touch changed rows.

---

## Key Capabilities

- FIRST_RUN toggle to control full build vs. incremental updates.
- xxhash64 row digest stored as BINARY(8) (row_hash) on each table.
- Hash‑guarded upserts via ON DUPLICATE KEY UPDATE, updating only when row_hash differs.
- Fast ingestion with LOAD DATA LOCAL INFILE (chunked temp files).
- Low‑RAM operation: reads TSVs in windows (CHUNK_ROWS), avoids dtype inflation (strings by default).
- Index management on first run to accelerate bulk loads.
- Robust logging with a rotating file handler (file + console).
- Genre normalization without JSON_TABLE using a recursive split in SQL; fills genres and title_genres lookup tables.

---

## Data Flow (High Level)

1. **Download** IMDb gz files from `https://datasets.imdbws.com/`.
2. **Extract** to TSV.
3. **Chunk** TSV → compute `xxhash64` per row (hex during temp stage).
4. **Stage** into temp table via `LOAD DATA LOCAL INFILE`.
5. **Convert** hex → `BINARY(8)` and **upsert** into target with hash guard.
6. **Normalize** genres (`title_basics.genres` → `genres` & `title_genres`).

---

## Project Structure

```
imdb_etl.py              # Main script (this repo)
README.md                # You are here
logs/imdb_sync_logs.txt  # Rotating ETL logs (path configurable)
imdb_files/
  new_download/          # Fresh .tsv.gz files
  new_extract/           # Extracted .tsv files
```

---

## Requirements

- **Python** 3.9+
- **MySQL** 8.0+ (tested) with `local_infile` enabled
- Packages:
  - `pandas`, `sqlalchemy`, `pymysql`, `xxhash`, `requests`, `tqdm`

Install packages:

```bash
pip install pandas SQLAlchemy pymysql xxhash requests
```

---

## MySQL Setup (ENABLE `LOCAL INFILE`)

This pipeline uses LOAD DATA LOCAL INFILE. Ensure both the server and client/connector allow it.

**Option A — via config file** (recommended)

In your MySQL config (e.g., `my.cnf` or `my.ini`):

```
[mysqld]
local_infile = 1

[client]
local-infile = 1
```

Restart MySQL after changes.

**Option B — at runtime** (requires sufficient privileges)

```sql
SET GLOBAL local_infile = 1;
```

Also ensure your SQLAlchemy URL enables it, e.g.:

```
mysql+pymysql://USERNAME:PASSWORD@HOST:3306/imdb?local_infile=1&charset=utf8mb4
```

> Security note: Only enable `LOCAL INFILE` if you trust the source and environment.

---

## Configuration

Edit paths and DB URI near the top of the script:

```python
NEW_DOWNLOAD_DIR = Path(r"D:\LLMs\imdb\imdb_files\new_download")
NEW_EXTRACT_DIR  = Path(r"D:\LLMs\imdb\imdb_files\new_extract")
LOG_PATH         = Path(r"D:\LLMs\imdb\logs\imdb_sync_logs.txt")

DB_URI = "mysql+pymysql://USERNAME:PASSWORD@localhost:3306/imdb?local_infile=1&charset=utf8mb4"
CHUNK_ROWS = int(os.getenv("CHUNK_ROWS", "150000"))
FIRST_RUN = True  # flip to False after first successful build
```

Environment override for chunk size:

```bash
# Example: smaller chunks for limited RAM
set CHUNK_ROWS=75000   # Windows (cmd)
export CHUNK_ROWS=75000 # Linux/macOS
```

---

## Running the Pipeline

**First run (schema + full load):**

1. Set `FIRST_RUN = True`.
2. Ensure MySQL `local_infile` is enabled (see above).
3. Run the script:

```bash
python imdb_etl.py
```

On success, flip `FIRST_RUN = False` for future runs.

**Incremental updates:**

- With `FIRST_RUN = False`, each execution:
  - recomputes `row_hash` per incoming row chunk,
  - upserts into target tables only when the incoming hash differs,
  - refreshes genre mappings (idempotent).

Logs go to both console and `LOG_PATH` with rotation enabled.

---

## Tables Created

- `name_basics (PK: nconst, row_hash)`
- `title_basics (PK: tconst, row_hash)`
- `title_crew (PK: tconst, row_hash)`
- `title_episode (PK: tconst, row_hash)`
- `title_principals (PK: tconst, ordering, row_hash)`
- `title_ratings (PK: tconst, row_hash)`
- `genres (genre_id, genre_name UNIQUE)`
- `title_genres (tconst, genre_id)`

Selected secondary indexes (re)created after first run for lookup speed (e.g., `name_basics.primaryName`, `title_basics.startYear`, `title_basics.genres`).

---

## How Hash‑Guarded Upserts Work

During staging we load a hex hash and then convert to BINARY(8) in MySQL:

```sql
UPDATE staging SET row_hash = UNHEX(row_hash_hex);
```

Upsert only updates when the incoming row_hash differs from the existing row:

```sql
INSERT INTO target (...)
VALUES (...)
ON DUPLICATE KEY UPDATE
  colX = IF(VALUES(row_hash) <> target.row_hash, VALUES(colX), target.colX),
  row_hash = IF(VALUES(row_hash) <> target.row_hash, VALUES(row_hash), target.row_hash);
```

This avoids unnecessary writes and keeps incremental runs fast.

---

## Genre Normalization

The IMDb title_basics.genres column stores comma‑separated genre tokens. We use a recursive CTE to split values safely (skipping null \N blanks) and populate:

- genres (genre_id, genre_name) with distinct tokens
- title_genres (tconst, genre_id) as the junction

---

## Performance Tips

- Start with larger CHUNK_ROWS (e.g., 150k) and tune based on RAM.
- Keep FIRST_RUN=True only for the initial build; it drops secondary indexes to speed bulk insert, then rebuilds them once.
- Ensure innodb_flush_log_at_trx_commit and related settings are sensible for your environment.
- Put the data directory and temp files on a fast SSD if possible.

---

## Logging & Observability

- Rotating logs via RotatingFileHandler (up to ~5 MB x 3 backups).
- Both console and file output with timestamps and levels.
- Clear start/finish markers per table and per mode (FIRST_RUN vs incremental).

---

## Roadmap Ideas (Portfolio Enhancements)

- Add metrics per stage (rows/s, elapsed) and export to Prometheus.
- Optional Dask parallelization for multi‑file ingestion (kept out here for clarity).
- CDC snapshots and historical audit tables (SCD Type 2).
- Docker Compose for one‑command local setup (MySQL + ETL).

---

## Credits

- IMDb datasets: © IMDb. Use subject to IMDb terms.
- Hashing: [`xxhash`](https://cyan4973.github.io/xxHash/).

---

## Quickstart TL;DR

1) Enable local_infile on MySQL.  
2) Set paths + DB_URI in the script.  
3) Run with FIRST_RUN=True once → flip to False.  
4) Schedule incremental runs (e.g., weekly).


