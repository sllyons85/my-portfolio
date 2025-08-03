"""
IMDb Incremental ETL Script
----------------------------
Downloads and extracts the latest IMDb .tsv.gz files, compares them with previously uploaded
versions, and performs incremental updates to a MySQL database. Designed to avoid full reloads
by inserting only new or updated rows. Maintains a backup of previously loaded data.

Intended as a professional portfolio project to demonstrate skills in ETL, data normalization,
and automation.
"""

import gzip
import hashlib
import os
import shutil
import sys
from contextlib import redirect_stdout, redirect_stderr

import pandas as pd
from sqlalchemy import create_engine, text
from tqdm import tqdm

# --- Configuration ---

NEW_DOWNLOAD_DIR = r"D:\\LLMs\\imdb\\imdb_files\\new_download"
NEW_EXTRACT_DIR = r"D:\\LLMs\\imdb\\imdb_files\\new_extract"
PREVIOUS_UPLOAD_DIR = r"D:\\LLMs\\imdb\\imdb_files\\previously_uploaded"
LOG_PATH = r"D:\\LLMs\\imdb\\logs\\imdb_sync_logs.txt"

DB_URI = "mysql+pymysql://username:password@localhost:3306/imdb"
engine = create_engine(DB_URI)

CHUNK_SIZE = 100_000
FILES = [
    "name.basics.tsv.gz",
    "title.basics.tsv.gz",
    "title.crew.tsv.gz",
    "title.episode.tsv.gz",
    "title.principals.tsv.gz",
    "title.ratings.tsv.gz"
]

# --- Utility Functions ---

def download_file(file_name):
    """Download IMDb .tsv.gz file from official dataset URL."""
    url = f"https://datasets.imdbws.com/{file_name}"
    out_path = os.path.join(NEW_DOWNLOAD_DIR, file_name)
    print(f"Downloading {file_name}")
    os.system(f"curl -o {out_path} {url}")

def extract_file(file_name):
    """Extract .gz file to .tsv format into NEW_EXTRACT_DIR."""
    gz_path = os.path.join(NEW_DOWNLOAD_DIR, file_name)
    tsv_path = os.path.join(NEW_EXTRACT_DIR, file_name.replace('.gz', ''))
    print(f"Extracting {file_name}")
    with gzip.open(gz_path, 'rb') as f_in, open(tsv_path, 'wb') as f_out:
        shutil.copyfileobj(f_in, f_out)

def clean_dataframe(df):
    """Replace IMDb-style nulls with pandas NA."""
    return df.replace('\\N', pd.NA)

def hash_row(row):
    """Generate a consistent hash of a row for change detection."""
    return hashlib.md5(pd.util.hash_pandas_object(row, index=False).values.tobytes()).hexdigest()

# --- Incremental Sync Logic ---

def sync_table(tsv_file, table_name):
    """Compare new data with previous and insert new/updated rows using REPLACE."""
    print(f"\nSyncing table: {table_name}")
    new_df = pd.read_csv(tsv_file, sep='\t', dtype=str, na_values='\\N')
    new_df = clean_dataframe(new_df)

    prev_file = os.path.join(PREVIOUS_UPLOAD_DIR, os.path.basename(tsv_file))
    if not os.path.exists(prev_file):
        print(f"No previous file found. Loading full data for {table_name}.")
        new_df.to_sql(table_name, con=engine, if_exists='append', index=False, method='multi')
        return

    prev_df = pd.read_csv(prev_file, sep='\t', dtype=str, na_values='\\N')
    prev_df = clean_dataframe(prev_df)

    if set(new_df.columns) != set(prev_df.columns):
        print(f"Column mismatch in {table_name}. Skipping incremental sync.")
        return

    new_df["__hash__"] = new_df.apply(hash_row, axis=1)
    prev_df["__hash__"] = prev_df.apply(hash_row, axis=1)

    key_cols = new_df.columns.difference(["__hash__"])
    merged = new_df.merge(prev_df, on=list(key_cols), how='left', indicator=True)
    new_or_updated = merged[merged['_merge'] == 'left_only'][list(new_df.columns)]

    if not new_or_updated.empty:
        print(f"{len(new_or_updated)} new or updated rows for {table_name}.")
        with engine.begin() as conn:
            for _, row in new_or_updated.iterrows():
                keys = row[key_cols].to_dict()
                conn.execute(text(
                    f"""REPLACE INTO {table_name} ({','.join(keys.keys())})
                         VALUES ({','.join([f':{k}' for k in keys])})"""
                ), keys)
    else:
        print(f"No changes detected for {table_name}.")

def update_previous_files():
    """Copy current extracted files into previous_upload folder for future comparisons."""
    print("Updating previously_uploaded directory...")
    for file_name in FILES:
        tsv_name = file_name.replace('.gz', '')
        src = os.path.join(NEW_EXTRACT_DIR, tsv_name)
        dst = os.path.join(PREVIOUS_UPLOAD_DIR, tsv_name)
        shutil.copyfile(src, dst)

# --- Metadata Normalization ---

def normalize_genres():
    """Split genre strings into separate records and populate relational genre tables."""
    print("Normalizing genres")
    with engine.begin() as conn:
        genres_df = pd.read_sql("SELECT tconst, genres FROM title_basics WHERE genres IS NOT NULL", conn)

        genre_set = set()
        mapping = []

        for _, row in genres_df.iterrows():
            tconst = row['tconst']
            genres = row['genres'].split(',')
            for g in genres:
                genre_set.add(g)
                mapping.append((tconst, g))

        for g in genre_set:
            conn.execute(text("INSERT IGNORE INTO genres (genre_name) VALUES (:g)"), {"g": g})

        genre_rows = conn.execute(text("SELECT * FROM genres")).fetchall()
        genre_map = {row.genre_name: row.genre_id for row in genre_rows}

        for tconst, g in mapping:
            genre_id = genre_map.get(g)
            if genre_id:
                conn.execute(text("""
                    INSERT IGNORE INTO title_genres (tconst, genre_id)
                    VALUES (:tconst, :genre_id)
                """), {"tconst": tconst, "genre_id": genre_id})

# --- Optimization ---

def add_indexes():
    """Create indexes to improve query performance."""
    print("Adding indexes")
    with engine.begin() as conn:
        conn.execute(text("CREATE INDEX idx_primaryName ON name_basics (primaryName(100))"))
        conn.execute(text("CREATE INDEX idx_start_year ON title_basics (startYear)"))
        conn.execute(text("CREATE INDEX idx_genres ON title_basics (genres)"))

# --- Workflow ---

def download_and_extract():
    """Download and extract all IMDb data files."""
    for file_name in FILES:
        download_file(file_name)
        extract_file(file_name)

def main():
    """Main entry point: download, extract, sync, normalize, index, and update previous."""
    print("Starting IMDb ETL sync...")
    download_and_extract()

    for file_name in FILES:
        tsv_path = os.path.join(NEW_EXTRACT_DIR, file_name.replace('.gz', ''))
        table_name = file_name.replace('.tsv.gz', '').replace('.', '_')
        sync_table(tsv_path, table_name)

    normalize_genres()
    add_indexes()
    update_previous_files()

    print("IMDb ETL sync complete.")

# --- Run Script with Logging ---

if __name__ == "__main__":
    os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)
    with open(LOG_PATH, "w", encoding="utf-8") as f:
        with redirect_stdout(f), redirect_stderr(f):
            main()
