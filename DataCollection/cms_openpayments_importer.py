import os
import zipfile
import requests
import csv
import psycopg2
from tqdm import tqdm
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

def get_latest_open_payments_url():
    base_url = "https://download.cms.gov/openpayments/"
    current_year = datetime.now().year

    for year in range(current_year - 1, 2012, -1):
        pgyr = f"PGYR{year}"
        filename = f"{pgyr}_P01302025_01212025.zip"
        url = f"{base_url}{filename}"
        response = requests.head(url)
        if response.status_code == 200:
            print(f"✅ Found dataset for year {year}: {url}")
            return url, filename, year
    raise Exception("❌ No dataset found.")

def download_file(url, save_path):
    print(f"⬇️ Downloading {url}")
    response = requests.get(url, stream=True)
    response.raise_for_status()
    total = int(response.headers.get("content-length", 0))
    with open(save_path, 'wb') as f, tqdm(total=total, unit='B', unit_scale=True) as bar:
        for chunk in response.iter_content(chunk_size=1024):
            f.write(chunk)
            bar.update(len(chunk))
    print("✅ Download complete")

def extract_zip(zip_path, extract_dir):
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_dir)
    print(f"📂 Extracted contents to: {extract_dir}")

def normalize_column(col):
    return col.strip().lower().replace(" ", "_").replace("-", "_").replace("(", "").replace(")", "").replace("/", "_").replace(".", "").replace("__", "_")

def create_table(cur, table_name, columns):
    col_defs = [f'"{normalize_column(col)}" TEXT' for col in columns]
    create_sql = f'''
        CREATE TABLE IF NOT EXISTS {table_name} (
            id SERIAL PRIMARY KEY,
            {', '.join(col_defs)}
        );
    '''
    cur.execute(create_sql)

def insert_batch(cur, table_name, columns, batch):
    col_names = [f'"{normalize_column(col)}"' for col in columns]
    placeholders = ', '.join(['%s'] * len(columns))
    query = f"INSERT INTO {table_name} ({', '.join(col_names)}) VALUES ({placeholders})"
    cur.executemany(query, batch)

def import_csv_to_table(cur, filepath, table_name, chunk_size=1000):
    with open(filepath, newline='', encoding="utf-8") as f:
        reader = csv.DictReader(f)
        columns = reader.fieldnames
        create_table(cur, table_name, columns)
        batch = []
        for i, row in enumerate(reader, 1):
            values = [row.get(col, None) for col in columns]
            batch.append(values)
            if len(batch) >= chunk_size:
                insert_batch(cur, table_name, columns, batch)
                batch.clear()
        if batch:
            insert_batch(cur, table_name, columns, batch)

def main():
    conn = None
    cur = None
    url, filename, year = get_latest_open_payments_url()
    data_dir = os.path.join(os.path.dirname(__file__), "data")
    os.makedirs(data_dir, exist_ok=True)
    zip_path = os.path.join(data_dir, filename)

    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST"),
            database=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD")
        )
        cur = conn.cursor()

        cur.execute("CREATE TABLE IF NOT EXISTS cms_open_payments_import_log (import_year TEXT PRIMARY KEY, file_name TEXT, imported_at TIMESTAMP DEFAULT now());")
        cur.execute("SELECT 1 FROM cms_open_payments_import_log WHERE import_year = %s", (str(year),))
        if cur.fetchone():
            print(f"✅ Data for {year} already imported. Skipping.")
            return

        if not os.path.exists(zip_path):
            download_file(url, zip_path)

        extract_zip(zip_path, data_dir)

        for file in os.listdir(data_dir):
            if file.endswith(".csv"):
                full_path = os.path.join(data_dir, file)
                if file.startswith("OP_DTL_GNRL_"):
                    print(f"📥 Importing General Payments – {file}")
                    import_csv_to_table(cur, full_path, "cms_open_payments_general_all")
                elif file.startswith("OP_DTL_RSRCH_"):
                    print(f"📥 Importing Research Payments – {file}")
                    import_csv_to_table(cur, full_path, "cms_open_payments_research_all")
                elif file.startswith("OP_DTL_OWNRSHP_"):
                    print(f"📥 Importing Ownership Payments – {file}")
                    import_csv_to_table(cur, full_path, "cms_open_payments_ownership_all")

        cur.execute("INSERT INTO cms_open_payments_import_log (import_year, file_name) VALUES (%s, %s)", (str(year), filename))
        conn.commit()

    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

if __name__ == "__main__":
    main()