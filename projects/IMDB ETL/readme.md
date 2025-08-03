# IMDb Incremental ETL Pipeline

This project is a professional-grade **incremental ETL pipeline** for IMDb datasets. It demonstrates skills in data engineering, automation, and database design. The pipeline is built in Python and loads data into a MySQL database, using a smart diffing system to avoid redundant data loads.

---

## Features

- **Incremental Loads**: Detects new or updated records using row hashing and only syncs what’s changed
- **Genre Normalization**: Splits genre strings into a relational structure with proper foreign keys
- **Indexing**: Adds indexes to improve downstream query performance
- **Logging**: Logs all operations to a file for reproducibility and debugging

---

## Project Structure

- `new_download/`: Raw IMDb .tsv.gz files downloaded from IMDb
- `new_extract/`: Extracted TSV files (uncompressed)
- `previously_uploaded/`: Reference for change detection (previous load)
- `logs/imdb_sync_logs.txt`: Log file
- `imdb_etl_script.py`: Main ETL script
- `README.md`: This file

---

## Datasets Used
All data is downloaded from the official [IMDb dataset repository](https://www.imdb.com/interfaces/):

- `name.basics.tsv.gz`
- `title.basics.tsv.gz`
- `title.crew.tsv.gz`
- `title.episode.tsv.gz`
- `title.principals.tsv.gz`
- `title.ratings.tsv.gz`

---

## Tech Stack

- **Python**: Core ETL logic using `pandas`, `sqlalchemy`, and `tqdm`
- **MySQL**: Target data warehouse
- **Shell tools**: Uses `curl` for downloads
- **Logging**: Logs captured via `redirect_stdout`

---

## Workflow

1. **Download & Extract**: Fetches `.tsv.gz` files and unzips them
2. **Compare**: Checks for row changes against previously loaded data
3. **Sync**: Uses SQL `REPLACE` to insert new or updated rows
4. **Normalize Genres**: Parses genre strings into a relational structure
5. **Indexing**: Creates indexes for performance
6. **Backup**: Copies current files into `previously_uploaded/` for future comparisons

---

## How to Run

1. Install required packages:
   ```bash
   pip install pandas sqlalchemy tqdm pymysql
   ```

2. Set up your MySQL database (use the schema defined in the script).
3. Update paths in the config section of `imdb_etl_script.py`.
4. Run the script:
   ```bash
   python imdb_etl_script.py
   ```

---

## Future Enhancements

- Add scheduling via Airflow or Prefect
- Visual dashboard using Streamlit or Superset
- Unit testing for ETL logic (pytest)
- Dockerize the environment for easier deployment

---

## Author
Built by Scott Lyons as part of a personal portfolio to demonstrate real-world data engineering skills.

Feel free to fork, reuse, or contact me for collaboration.

---
