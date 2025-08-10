"""
IMDb Incremental ETL (xxhash64, low-RAM, chunked)
-------------------------------------------------
- FIRST_RUN toggle in-script (set True for the very first full build)
- Uses LOAD DATA LOCAL INFILE for speed (first-run + incremental chunk loads)
- Stores per-row xxhash64 in BINARY(8) column 'row_hash' on each table
- Upserts only changed/new rows using ON DUPLICATE KEY UPDATE with hash guard
- Drops/rebuilds secondary indexes during first run for faster bulk load
- Genre normalization via recursive split (no JSON_TABLE / encoding pitfalls)
"""

from __future__ import annotations

import csv
import gzip
import logging
from logging.handlers import RotatingFileHandler
import os
import shutil
import tempfile
from pathlib import Path
from typing import Iterable, List

import pandas as pd
import xxhash
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
import requests

# -------------------- Configuration --------------------

NEW_DOWNLOAD_DIR = Path(r"D:\LLMs\imdb\imdb_files\new_download")
NEW_EXTRACT_DIR  = Path(r"D:\LLMs\imdb\imdb_files\new_extract")
LOG_PATH         = Path(r"D:\LLMs\imdb\logs\imdb_sync_logs.txt")

# Enable local_infile=1 for LOAD DATA LOCAL INFILE
DB_URI = "mysql+pymysql://root:De852054!@localhost:3306/imdb?local_infile=1&charset=utf8mb4"
engine: Engine = create_engine(
    DB_URI,
    pool_pre_ping=True,
    pool_recycle=3600,
    future=True,
    connect_args={"local_infile": 1},
)

FILES = [
    "name.basics.tsv.gz",
    "title.basics.tsv.gz",
    "title.crew.tsv.gz",
    "title.episode.tsv.gz",
    "title.principals.tsv.gz",
    "title.ratings.tsv.gz",
]

# Tunables
CHUNK_ROWS = int(os.getenv("CHUNK_ROWS", "150000"))
TIMEOUT    = (10, 300)  # (connect, read) for downloads

# Toggle full rebuild vs. incremental
FIRST_RUN = True  # Set to False after the first run

# -------------------- Logging --------------------

os.makedirs(LOG_PATH.parent, exist_ok=True)
logger = logging.getLogger("imdb_etl")
logger.setLevel(logging.INFO)

# prevent duplicate handlers if script is re-imported
if not logger.handlers:
    handler = RotatingFileHandler(LOG_PATH, maxBytes=5_000_000, backupCount=3, encoding="utf-8")
    fmt = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    handler.setFormatter(fmt)
    logger.addHandler(handler)

    console = logging.StreamHandler()
    console.setFormatter(fmt)
    logger.addHandler(console)

# -------------------- Helpers --------------------


def download_file(file_name: str) -> Path:
    url = f"https://datasets.imdbws.com/{file_name}"
    out_path = NEW_DOWNLOAD_DIR / file_name
    out_path.parent.mkdir(parents=True, exist_ok=True)
    logger.info(f"Downloading {file_name} -> {out_path}")
    with requests.get(url, stream=True, timeout=TIMEOUT) as r:
        r.raise_for_status()
        with open(out_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)
    return out_path


def extract_gz(gz_path: Path) -> Path:
    tsv_path = NEW_EXTRACT_DIR / gz_path.name.replace(".gz", "")
    tsv_path.parent.mkdir(parents=True, exist_ok=True)
    logger.info(f"Extracting {gz_path.name} -> {tsv_path}")
    with gzip.open(gz_path, "rb") as f_in, open(tsv_path, "wb") as f_out:
        shutil.copyfileobj(f_in, f_out)
    return tsv_path


def normalize_nulls(df: pd.DataFrame) -> pd.DataFrame:
    # IMDb uses \N for nulls; keep everything as string to avoid type inflation
    df = df.replace("\\N", None)
    return df


def xxhash64_row(values: Iterable[str]) -> bytes:
    """Compute xxhash64 over a row (stable, low RAM). Returns 8-byte digest."""
    h = xxhash.xxh64()
    first = True
    for v in values:
        s = "" if v is None or (isinstance(v, float) and pd.isna(v)) else str(v)
        if not first:
            h.update("\x1f")  # unit separator to prevent accidental concatenation collisions
        h.update(s)
        first = False
    return h.digest()  # 8 bytes


def compute_hashes_and_write_chunk(tsv_path: Path, columns: List[str], out_csv: Path,
                                   start: int, nrows: int) -> int:
    """Read a slice (start,nrows) from TSV -> write CSV with row_hash (hex) column.
       Returns number of rows written."""
    # Read a window of rows (skip header + previous rows)
    df = pd.read_csv(
        tsv_path,
        sep="\t",
        dtype=str,
        na_values="\\N",
        keep_default_na=False,  # keep empty strings as empty
        quoting=csv.QUOTE_NONE,
        on_bad_lines="skip",
        skiprows=range(1, start + 1) if start > 0 else None,
        nrows=nrows,
        engine="c",
    )
    if df.empty:
        return 0

    df = normalize_nulls(df)

    # Keep/align columns (drop unexpected gracefully)
    present = [c for c in columns if c in df.columns]
    df = df[present]

    # Compute row_hash per row without materializing a giant string
    hashes_hex = []
    for row_vals in df.itertuples(index=False, name=None):
        digest = xxhash64_row(row_vals)  # bytes
        hashes_hex.append(digest.hex())
    df.insert(0, "row_hash_hex", hashes_hex)

    # Write CSV for LOAD DATA (headerless)
    df.to_csv(out_csv, index=False, header=False, sep="\t", quoting=csv.QUOTE_NONE, na_rep="")
    return len(df)


def load_chunk_with_load_data(conn, table: str, tmp_csv: Path, columns: List[str]) -> None:
    """LOAD DATA LOCAL INFILE into a staging table, then upsert into target."""
    staging = f"{table}__staging"

    # Create staging (no PK) mirrors target columns + row_hash
    conn.execute(text(f"DROP TEMPORARY TABLE IF EXISTS `{staging}`"))
    create_cols = ", ".join([f"`{c}` TEXT NULL" for c in columns])
    conn.execute(text(
        f"""
        CREATE TEMPORARY TABLE `{staging}` (
            `row_hash` BINARY(8) NOT NULL,
            {create_cols}
        ) ENGINE=InnoDB
        """
    ))

    # LOAD into staging: first column is hex -> we store to a helper then convert
    conn.execute(text(f"ALTER TABLE `{staging}` ADD COLUMN `row_hash_hex` VARCHAR(16) NULL"))
    cols_with_hex = ["row_hash_hex"] + columns
    cols_list = ", ".join(f"`{c}`" for c in cols_with_hex)

    logger.debug(f"LOAD DATA -> {staging} from {tmp_csv}")
    conn.execute(text(
        f"""
        LOAD DATA LOCAL INFILE :path
        INTO TABLE `{staging}`
        CHARACTER SET utf8mb4
        FIELDS TERMINATED BY '\t' ESCAPED BY '' ENCLOSED BY ''
        LINES TERMINATED BY '\n'
        ({cols_list})
        """
    ), {"path": str(tmp_csv)})

    # Convert hex to binary and drop hex column
    conn.execute(text(f"UPDATE `{staging}` SET `row_hash` = UNHEX(`row_hash_hex`)"))
    conn.execute(text(f"ALTER TABLE `{staging}` DROP COLUMN `row_hash_hex`"))

    # Upsert from staging into target, updating only when hash differs
    set_list = ", ".join([f"`{c}` = IF(VALUES(`row_hash`) <> `{table}`.`row_hash`, VALUES(`{c}`), `{table}`.`{c}`)" for c in columns])
    sql = f"""
        INSERT INTO `{table}` (`row_hash`, {", ".join(f"`{c}`" for c in columns)})
        SELECT `row_hash`, {", ".join(f"`{c}`" for c in columns)} FROM `{staging}`
        ON DUPLICATE KEY UPDATE
            {set_list},
            `row_hash` = IF(VALUES(`row_hash`) <> `{table}`.`row_hash`, VALUES(`row_hash`), `{table}`.`row_hash`);
    """
    conn.execute(text(sql))


def table_columns_for(file_base: str) -> List[str]:
    """Explicit column lists per IMDb TSV (excluding 'row_hash')."""
    if file_base == "name.basics":
        return ["nconst", "primaryName", "birthYear", "deathYear", "primaryProfession", "knownForTitles"]
    if file_base == "title.basics":
        return ["tconst", "titleType", "primaryTitle", "originalTitle", "isAdult", "startYear", "endYear", "runtimeMinutes", "genres"]
    if file_base == "title.crew":
        return ["tconst", "directors", "writers"]
    if file_base == "title.episode":
        return ["tconst", "parentTconst", "seasonNumber", "episodeNumber"]
    if file_base == "title.principals":
        return ["tconst", "ordering", "nconst", "category", "job", "characters"]
    if file_base == "title.ratings":
        return ["tconst", "averageRating", "numVotes"]
    raise ValueError(file_base)


def target_table_name(file_base: str) -> str:
    return file_base.replace(".", "_")


# -------------------- DDL --------------------

DDL = {
    "name_basics": """
        CREATE TABLE `name_basics` (
            `nconst` VARCHAR(12) NOT NULL,
            `primaryName` VARCHAR(255) NULL,
            `birthYear` VARCHAR(10) NULL,
            `deathYear` VARCHAR(10) NULL,
            `primaryProfession` VARCHAR(255) NULL,
            `knownForTitles` TEXT NULL,
            `row_hash` BINARY(8) NOT NULL,
            PRIMARY KEY (`nconst`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
    "title_basics": """
        CREATE TABLE `title_basics` (
            `tconst` VARCHAR(12) NOT NULL,
            `titleType` VARCHAR(32) NULL,
            `primaryTitle` VARCHAR(1024) NULL,
            `originalTitle` VARCHAR(1024) NULL,
            `isAdult` VARCHAR(4) NULL,
            `startYear` VARCHAR(10) NULL,
            `endYear` VARCHAR(10) NULL,
            `runtimeMinutes` VARCHAR(10) NULL,
            `genres` VARCHAR(255) NULL,
            `row_hash` BINARY(8) NOT NULL,
            PRIMARY KEY (`tconst`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
    "title_crew": """
        CREATE TABLE `title_crew` (
            `tconst` VARCHAR(12) NOT NULL,
            `directors` TEXT NULL,
            `writers` TEXT NULL,
            `row_hash` BINARY(8) NOT NULL,
            PRIMARY KEY (`tconst`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
    "title_episode": """
        CREATE TABLE `title_episode` (
            `tconst` VARCHAR(12) NOT NULL,
            `parentTconst` VARCHAR(12) NULL,
            `seasonNumber` VARCHAR(10) NULL,
            `episodeNumber` VARCHAR(10) NULL,
            `row_hash` BINARY(8) NOT NULL,
            PRIMARY KEY (`tconst`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
    "title_principals": """
        CREATE TABLE `title_principals` (
            `tconst` VARCHAR(12) NOT NULL,
            `ordering` VARCHAR(10) NOT NULL,
            `nconst` VARCHAR(12) NULL,
            `category` VARCHAR(64) NULL,
            `job` TEXT NULL,
            `characters` TEXT NULL,
            `row_hash` BINARY(8) NOT NULL,
            PRIMARY KEY (`tconst`, `ordering`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
    "title_ratings": """
        CREATE TABLE `title_ratings` (
            `tconst` VARCHAR(12) NOT NULL,
            `averageRating` VARCHAR(8) NULL,
            `numVotes` VARCHAR(16) NULL,
            `row_hash` BINARY(8) NOT NULL,
            PRIMARY KEY (`tconst`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
    "genres": """
        CREATE TABLE `genres` (
            `genre_id` INT AUTO_INCREMENT PRIMARY KEY,
            `genre_name` VARCHAR(64) NOT NULL UNIQUE
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
    "title_genres": """
        CREATE TABLE `title_genres` (
            `tconst` VARCHAR(12) NOT NULL,
            `genre_id` INT NOT NULL,
            PRIMARY KEY (`tconst`, `genre_id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
}

SECONDARY_INDEXES = [
    # name_basics
    "CREATE INDEX `idx_primaryName` ON `name_basics` (`primaryName`(100))",
    # title_basics
    "CREATE INDEX `idx_startYear` ON `title_basics` (`startYear`)",
    "CREATE INDEX `idx_genres` ON `title_basics` (`genres`)",
]


def drop_and_create_all_tables():
    with engine.begin() as conn:
        logger.info("Dropping & creating tables (first run)...")
        for tbl in ["title_genres", "genres",
                    "title_ratings", "title_principals", "title_episode",
                    "title_crew", "title_basics", "name_basics"]:
            conn.execute(text(f"DROP TABLE IF EXISTS `{tbl}`"))
        for name in ["name_basics", "title_basics", "title_crew", "title_episode", "title_principals", "title_ratings", "genres", "title_genres"]:
            conn.execute(text(DDL[name]))


def drop_secondary_indexes():
    with engine.begin() as conn:
        logger.info("Dropping secondary indexes (if exist)...")
        for stmt in [
            "DROP INDEX `idx_primaryName` ON `name_basics`",
            "DROP INDEX `idx_startYear` ON `title_basics`",
            "DROP INDEX `idx_genres` ON `title_basics`",
        ]:
            try:
                conn.execute(text(stmt))
            except Exception:
                pass


def create_secondary_indexes():
    with engine.begin() as conn:
        logger.info("Creating secondary indexes...")
        for stmt in SECONDARY_INDEXES:
            conn.execute(text(stmt))


# -------------------- Loaders --------------------

def bulk_first_load(tsv_path: Path, table: str, columns: List[str]) -> None:
    """Full load with row_hash via chunked temp files + LOAD DATA."""
    logger.info(f"[FIRST RUN] Bulk loading {table} from {tsv_path.name}")
    total = 0
    start = 0
    with engine.begin() as conn:
        while True:
            with tempfile.NamedTemporaryFile(mode="w+", suffix=".tsv", delete=False) as tmp:
                tmp_path = Path(tmp.name)
            try:
                n = compute_hashes_and_write_chunk(tsv_path, columns, tmp_path, start, CHUNK_ROWS)
                if n == 0:
                    break
                load_chunk_with_load_data(conn, table, tmp_path, columns)
                total += n
                start += n
                logger.info(f"Loaded {n} rows into {table} (total {total})")
            finally:
                if tmp_path.exists():
                    try:
                        tmp_path.unlink()
                    except Exception:
                        pass
    logger.info(f"[FIRST RUN] Completed load for {table}: {total} rows")


def incremental_upsert(tsv_path: Path, table: str, columns: List[str]) -> None:
    """Incremental: chunked upsert with hash guard."""
    logger.info(f"[INCR] Upserting {table} from {tsv_path.name} (chunk={CHUNK_ROWS})")
    total = 0
    start = 0
    with engine.begin() as conn:
        while True:
            with tempfile.NamedTemporaryFile(mode="w+", suffix=".tsv", delete=False) as tmp:
                tmp_path = Path(tmp.name)
            try:
                n = compute_hashes_and_write_chunk(tsv_path, columns, tmp_path, start, CHUNK_ROWS)
                if n == 0:
                    break
                load_chunk_with_load_data(conn, table, tmp_path, columns)
                total += n
                start += n
                logger.info(f"[INCR] Processed {n} rows for {table} (total {total})")
            finally:
                if tmp_path.exists():
                    try:
                        tmp_path.unlink()
                    except Exception:
                        pass
    logger.info(f"[INCR] Completed upsert for {table}: {total} rows processed")


# -------------------- Genre Normalization (JSON-free) --------------------

def normalize_genres_sql():
    """
    Populate genres and title_genres from title_basics.genres using a recursive
    split on commas (no JSON_TABLE). Skips NULL/blank/'\\N', trims tokens.
    Uses INSERT ... SELECT FROM (WITH ...) to satisfy MySQL syntax.
    """
    logger.info("Normalizing genres -> genres/title_genres (recursive split)")
    with engine.begin() as conn:
        # Ensure lookup/junction exist (idempotent if created earlier)
        conn.execute(text(DDL["genres"]))
        conn.execute(text(DDL["title_genres"]))

        # 1) Insert distinct genres
        conn.execute(text("""
            INSERT IGNORE INTO genres (genre_name)
            SELECT DISTINCT s.genre
            FROM (
              WITH RECURSIVE split AS (
                SELECT
                  tb.tconst,
                  TRIM(REPLACE(tb.genres, '\\N', '')) AS genres_clean,
                  1 AS pos,
                  TRIM(SUBSTRING_INDEX(REPLACE(tb.genres, '\\N',''), ',', 1)) AS genre
                FROM title_basics tb
                WHERE tb.genres IS NOT NULL AND tb.genres <> '' AND tb.genres <> '\\N'
                UNION ALL
                SELECT
                  tconst,
                  genres_clean,
                  pos + 1,
                  TRIM(
                    SUBSTRING_INDEX(
                      SUBSTRING_INDEX(genres_clean, ',', pos + 1),
                      ',', -1
                    )
                  ) AS genre
                FROM split
                WHERE pos < 3
              )
              SELECT genre
              FROM split
              WHERE genre IS NOT NULL AND genre <> ''
            ) AS s;
        """))

        # 2) Populate/refresh junction pairs
        conn.execute(text("""
            INSERT IGNORE INTO title_genres (tconst, genre_id)
            SELECT s.tconst, g.genre_id
            FROM (
              WITH RECURSIVE split AS (
                SELECT
                  tb.tconst,
                  TRIM(REPLACE(tb.genres, '\\N', '')) AS genres_clean,
                  1 AS pos,
                  TRIM(SUBSTRING_INDEX(REPLACE(tb.genres, '\\N',''), ',', 1)) AS genre
                FROM title_basics tb
                WHERE tb.genres IS NOT NULL AND tb.genres <> '' AND tb.genres <> '\\N'
                UNION ALL
                SELECT
                  tconst,
                  genres_clean,
                  pos + 1,
                  TRIM(
                    SUBSTRING_INDEX(
                      SUBSTRING_INDEX(genres_clean, ',', pos + 1),
                      ',', -1
                    )
                  ) AS genre
                FROM split
                WHERE pos < 3
              )
              SELECT DISTINCT tconst, genre
              FROM split
              WHERE genre IS NOT NULL AND genre <> ''
            ) AS s
            JOIN genres g
              ON g.genre_name = s.genre;
        """))


# -------------------- Workflow --------------------

def ensure_dirs():
    NEW_DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
    NEW_EXTRACT_DIR.mkdir(parents=True, exist_ok=True)


def download_and_extract_all() -> List[Path]:
    paths = []
    for file_name in FILES:
        gz = download_file(file_name)
        tsv = extract_gz(gz)
        paths.append(tsv)
    return paths


def process_all(tsv_paths: List[Path]):
    # Determine target + columns per file
    plan = []
    for tsv_path in tsv_paths:
        base = tsv_path.stem  # e.g., "name.basics"
        table = target_table_name(base)
        cols = table_columns_for(base)
        plan.append((tsv_path, table, cols))

    if FIRST_RUN:
        drop_secondary_indexes()
        drop_and_create_all_tables()
        # First-run: bulk load all, then indexes + genres
        for tsv_path, table, cols in plan:
            bulk_first_load(tsv_path, table, cols)
        create_secondary_indexes()
        normalize_genres_sql()
    else:
        # Incremental: upsert each table, then refresh genre mappings (safe + idempotent)
        for tsv_path, table, cols in plan:
            incremental_upsert(tsv_path, table, cols)
        normalize_genres_sql()


def main():
    logger.info("Starting IMDb ETL sync (xxhash64)")
    ensure_dirs()
    tsv_paths = download_and_extract_all()
    process_all(tsv_paths)
    logger.info("IMDb ETL sync complete.")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.exception("ETL failed")
        raise
