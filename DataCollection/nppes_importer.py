import csv
import io
import os
import re
import shutil
import zipfile
import requests
from tqdm import tqdm
import psycopg2
from datetime import datetime

def download_file(url, save_path):
    print(f"Attempting to download from: {url}")
    response = requests.get(url, stream=True)
    total_size_in_bytes = int(response.headers.get('content-length', 0))
    progress_bar = tqdm(total=total_size_in_bytes, unit='B', unit_scale=True)
    with open(save_path, 'wb') as file:
        for data in response.iter_content(chunk_size=1024):
            progress_bar.update(len(data))
            file.write(data)
    progress_bar.close()
    print('Archive file downloaded successfully.')

def find_csv_in_zip(zip_filename, pattern):
    with zipfile.ZipFile(zip_filename, 'r') as zip_file:
        for file_info in zip_file.infolist():
            if re.match(pattern, file_info.filename):
                return file_info.filename
    raise FileNotFoundError("No CSV file matching the pattern found in the zip archive.")

def extract_csv_from_zip(zip_filename, csv_filename):
    with zipfile.ZipFile(zip_filename, 'r') as zip_file:
        with zip_file.open(csv_filename) as csv_file_binary:
            csv_file_text = io.TextIOWrapper(csv_file_binary, encoding='utf-8')
            reader = csv.DictReader(csv_file_text)
            for row in reader:
                yield row

def cleanup_directory(directory):
    print('Cleaning up directory...')
    for filename in os.listdir(directory):
        file_path = os.path.join(directory, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
                print(f'Deleted file: {file_path}')
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
                print(f'Deleted directory: {file_path}')
        except Exception as e:
            print(f'Failed to delete {file_path}. Reason: {e}')
    print('Directory cleanup complete.')

def create_normalized_tables(cur):
    cur.execute("""
    CREATE TABLE IF NOT EXISTS providers (
        npi VARCHAR(10) PRIMARY KEY,
        entity_type_code TEXT,
        org_name TEXT,
        first_name TEXT,
        last_name TEXT,
        enumeration_date DATE,
        last_update_date DATE,
        gender TEXT,
        is_sole_proprietor BOOLEAN
    );

    CREATE TABLE IF NOT EXISTS provider_addresses (
        id SERIAL PRIMARY KEY,
        npi VARCHAR(10),
        address_type TEXT,
        address_1 TEXT,
        address_2 TEXT,
        city TEXT,
        state TEXT,
        postal_code TEXT,
        country_code TEXT,
        phone TEXT,
        fax TEXT
    );

    CREATE TABLE IF NOT EXISTS provider_taxonomies (
        id SERIAL PRIMARY KEY,
        npi VARCHAR(10),
        taxonomy_code TEXT,
        license_number TEXT,
        license_state TEXT,
        taxonomy_group TEXT,
        is_primary BOOLEAN
    );

    CREATE TABLE IF NOT EXISTS nppes_import_log (
        id SERIAL PRIMARY KEY,
        import_month TEXT UNIQUE,
        file_name TEXT,
        imported_at TIMESTAMP DEFAULT now()
    );
    """)

def parse_bool(value):
    return value.strip().upper() == 'Y' if value else False

def parse_date(value):
    try:
        return datetime.strptime(value, "%m/%d/%Y").date()
    except:
        return None

def normalize_and_insert(cur, row):
    npi = row.get("NPI")
    cur.execute("""
        INSERT INTO providers (npi, entity_type_code, org_name, first_name, last_name,
            enumeration_date, last_update_date, gender, is_sole_proprietor)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (npi) DO NOTHING;
    """, (
        npi,
        row.get("Entity Type Code"),
        row.get("Provider Organization Name (Legal Business Name)"),
        row.get("Provider First Name"),
        row.get("Provider Last Name (Legal Name)"),
        parse_date(row.get("Provider Enumeration Date")),
        parse_date(row.get("Last Update Date")),
        row.get("Provider Gender Code"),
        parse_bool(row.get("Is Sole Proprietor"))
    ))

    cur.execute("""
        INSERT INTO provider_addresses (npi, address_type, address_1, address_2, city, state, postal_code, country_code, phone, fax)
        VALUES (%s, 'practice', %s, %s, %s, %s, %s, %s, %s, %s);
    """, (
        npi,
        row.get("Provider First Line Business Practice Location Address"),
        row.get("Provider Second Line Business Practice Location Address"),
        row.get("Provider Business Practice Location Address City Name"),
        row.get("Provider Business Practice Location Address State Name"),
        row.get("Provider Business Practice Location Address Postal Code", '').strip()[:5],
        row.get("Provider Business Practice Location Address Country Code (If outside U.S.)"),
        row.get("Provider Business Practice Location Address Telephone Number"),
        row.get("Provider Business Practice Location Address Fax Number")
    ))

    for i in range(1, 6):
        code = row.get(f"Healthcare Provider Taxonomy Code_{i}")
        if code:
            cur.execute("""
                INSERT INTO provider_taxonomies (npi, taxonomy_code, license_number, license_state, taxonomy_group, is_primary)
                VALUES (%s, %s, %s, %s, %s, %s);
            """, (
                npi,
                code,
                row.get(f"Provider License Number_{i}"),
                row.get(f"Provider License Number State Code_{i}"),
                row.get(f"Healthcare Provider Taxonomy Group_{i}"),
                parse_bool(row.get(f"Healthcare Provider Primary Taxonomy Switch_{i}", 'N'))
            ))

def main():
    conn = None
    cur = None

    current_month_year = datetime.now().strftime("%B_%Y")
    download_url = f"https://download.cms.gov/nppes/NPPES_Data_Dissemination_{current_month_year}.zip"
    download_directory = os.path.join(os.path.dirname(__file__), 'data')
    archive_path = os.path.join(download_directory, f"NPPES_Data_Dissemination_{current_month_year}.zip")

    os.makedirs(download_directory, exist_ok=True)

    csv_filename_pattern = r'npidata_pfile_\d+-\d+\.csv'

    try:
        conn = psycopg2.connect(
            host="localhost",
            database="postgres",
            user="postgres",
            password="postgres"
        )
        cur = conn.cursor()

        create_normalized_tables(cur)
        conn.commit()

        cur.execute("SELECT 1 FROM nppes_import_log WHERE import_month = %s", (current_month_year,))
        if cur.fetchone():
            print(f"✅ NPPES data for {current_month_year} has already been imported. Skipping download and import.")
            return

        if not os.path.exists(archive_path):
            download_file(download_url, archive_path)

        csv_filename_inside_zip = find_csv_in_zip(archive_path, csv_filename_pattern)
        print(f"Found CSV file: {csv_filename_inside_zip}")

        for row in extract_csv_from_zip(archive_path, csv_filename_inside_zip):
            normalize_and_insert(cur, row)
        conn.commit()

        cur.execute("INSERT INTO nppes_import_log (import_month, file_name) VALUES (%s, %s)",
                    (current_month_year, csv_filename_inside_zip))
        conn.commit()

    except Exception as e:
        print(f"Error: {e}")
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
        cleanup_directory(download_directory)

if __name__ == "__main__":
    main()
