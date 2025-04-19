
import os
import csv
import requests
import psycopg2
from tqdm import tqdm
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

LEIE_URL = "https://oig.hhs.gov/exclusions/downloadables/UPDATED.csv"
DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
CSV_PATH = os.path.join(DATA_DIR, "UPDATED.csv")

def download_leie_file():
    os.makedirs(DATA_DIR, exist_ok=True)
    print(f"⬇️ Downloading LEIE data from: {LEIE_URL}")
    response = requests.get(LEIE_URL, stream=True)
    response.raise_for_status()
    total = int(response.headers.get("content-length", 0))
    with open(CSV_PATH, 'wb') as f, tqdm(total=total, unit='B', unit_scale=True) as bar:
        for chunk in response.iter_content(chunk_size=1024):
            f.write(chunk)
            bar.update(len(chunk))
    print("✅ Download complete")

def normalize_column(col):
    return col.strip().lower().replace(" ", "_").replace("-", "_").replace("/", "_")

def create_leie_table(cur, columns):
    col_defs = [f'"{normalize_column(col)}" TEXT' for col in columns]
    cur.execute(f'''
        CREATE TABLE IF NOT EXISTS hhs_leie_exclusions (
            id SERIAL PRIMARY KEY,
            {', '.join(col_defs)}
        );
    ''')

def insert_rows(cur, columns, rows):
    col_names = [f'"{normalize_column(col)}"' for col in columns]
    placeholders = ', '.join(['%s'] * len(columns))
    query = f'INSERT INTO hhs_leie_exclusions ({", ".join(col_names)}) VALUES ({placeholders})'
    cur.executemany(query, rows)

def import_leie_to_db(cur, chunk_size=1000):
    with open(CSV_PATH, newline='', encoding='latin-1') as f:
        reader = csv.DictReader(f)
        columns = reader.fieldnames
        create_leie_table(cur, columns)
        batch = []
        for row in reader:
            batch.append([row.get(col, None) for col in columns])
            if len(batch) >= chunk_size:
                insert_rows(cur, columns, batch)
                batch.clear()
        if batch:
            insert_rows(cur, columns, batch)

def create_import_log_table(cur):
    cur.execute('''
        CREATE TABLE IF NOT EXISTS hhs_leie_import_log (
            id SERIAL PRIMARY KEY,
            import_date TIMESTAMP DEFAULT now(),
            file_name TEXT
        );
    ''')

def main():
    conn = None
    cur = None

    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST"),
            database=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD")
        )
        cur = conn.cursor()

        create_import_log_table(cur)

        download_leie_file()
        import_leie_to_db(cur)

        cur.execute("INSERT INTO hhs_leie_import_log (file_name) VALUES (%s)", (os.path.basename(CSV_PATH),))
        conn.commit()

        print("✅ LEIE import complete.")

    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
        if os.path.exists(CSV_PATH):
            os.remove(CSV_PATH)
            print(f"🧹 Deleted downloaded file: {CSV_PATH}")

if __name__ == "__main__":
    main()
