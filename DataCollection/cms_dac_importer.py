import csv
import os
import shutil
import requests
from tqdm import tqdm
import psycopg2
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def get_latest_dac_url():
    api_url = "https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items/mj5m-pzi6?show-reference-ids=false"
    print(f"🔍 Requesting DAC dataset metadata from: {api_url}")
    response = requests.get(api_url)
    response.raise_for_status()
    data = response.json()

    for dist in data.get("distribution", []):
        url = dist.get("data", {}).get("downloadURL", "")
        if url.endswith("DAC_NationalDownloadableFile.csv"):
            print(f"✅ Found DAC CSV download URL:\n{url}")
            return url

    raise Exception("❌ DAC CSV URL not found in dataset metadata.")

def download_file(url, save_path):
    print(f"⬇️ Downloading: {url}")
    response = requests.get(url, stream=True)
    response.raise_for_status()
    total_size = int(response.headers.get("content-length", 0))

    with open(save_path, 'wb') as file, tqdm(total=total_size, unit='B', unit_scale=True) as bar:
        for chunk in response.iter_content(chunk_size=8192):
            file.write(chunk)
            bar.update(len(chunk))
    print('✅ Download complete.')

def cleanup_directory(directory):
    print('🧹 Cleaning up directory...')
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
    print('✅ Directory cleanup complete.')

def create_dac_tables(cur):
    cur.execute("""
    CREATE TABLE IF NOT EXISTS cms_dac_clinicians (
        id SERIAL PRIMARY KEY,
        npi VARCHAR(10),
        pac_id VARCHAR(10),
        enrollment_id VARCHAR(15),
        last_name TEXT,
        first_name TEXT,
        middle_name TEXT,
        suffix TEXT,
        gender TEXT,
        credential TEXT,
        medical_school TEXT,
        graduation_year INT,
        primary_specialty TEXT,
        secondary_specialty_1 TEXT,
        secondary_specialty_2 TEXT,
        secondary_specialty_3 TEXT,
        secondary_specialty_4 TEXT,
        all_secondary_specialties TEXT
    );

    CREATE TABLE IF NOT EXISTS cms_dac_practice_locations (
        id SERIAL PRIMARY KEY,
        npi VARCHAR(10),
        enrollment_id VARCHAR(15),
        group_pac_id VARCHAR(10),
        facility_name TEXT,
        num_org_members INT,
        telehealth BOOLEAN,
        address_1 TEXT,
        address_2 TEXT,
        address_suppressed BOOLEAN,
        city TEXT,
        state TEXT,
        zip TEXT,
        phone TEXT,
        accepts_assignment_individual TEXT,
        accepts_assignment_group TEXT,
        address_id VARCHAR(25)
    );

    CREATE TABLE IF NOT EXISTS cms_dac_import_log (
        id SERIAL PRIMARY KEY,
        import_month TEXT UNIQUE,
        file_name TEXT,
        imported_at TIMESTAMP DEFAULT now()
    );
    """)

def parse_bool(value):
    return value.strip().upper() == 'Y' if value else False

def normalize_and_insert(cur, row):
    # Clinicians table insert
    cur.execute("""
        INSERT INTO cms_dac_clinicians (
            npi, pac_id, enrollment_id, last_name, first_name, middle_name, suffix, gender,
            credential, medical_school, graduation_year, primary_specialty,
            secondary_specialty_1, secondary_specialty_2, secondary_specialty_3,
            secondary_specialty_4, all_secondary_specialties
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """, (
        row.get("NPI"),
        row.get("Ind_PAC_ID"),
        row.get("Ind_enrl_ID"),
        row.get("Provider Last Name"),
        row.get("Provider First Name"),
        row.get("Provider Middle Name"),
        row.get("suff"),
        row.get("gndr"),
        row.get("Cred"),
        row.get("Med_sch"),
        int(row.get("Grd_yr")) if row.get("Grd_yr") and row.get("Grd_yr").isdigit() else None,
        row.get("pri_spec"),
        row.get("sec_spec_1"),
        row.get("sec_spec_2"),
        row.get("sec_spec_3"),
        row.get("sec_spec_4"),
        row.get("sec_spec_all")
    ))

    # Practice locations insert
    cur.execute("""
        INSERT INTO cms_dac_practice_locations (
            npi, enrollment_id, group_pac_id, facility_name, num_org_members,
            telehealth, address_1, address_2, address_suppressed, city, state, zip, phone,
            accepts_assignment_individual, accepts_assignment_group, address_id
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """, (
        row.get("NPI"),
        row.get("Ind_enrl_ID"),
        row.get("org_pac_id"),
        row.get("Facility Name"),
        int(row.get("num_org_mem") or 0),
        parse_bool(row.get("Telehlth")),
        row.get("adr_ln_1"),
        row.get("adr_ln_2"),
        parse_bool(row.get("ln_2_sprs")),
        row.get("City/Town"),
        row.get("State"),
        row.get("ZIP Code"),
        row.get("Telephone Number"),
        row.get("ind_assgn"),
        row.get("grp_assgn"),
        row.get("adrs_id")
    ))

def main():
    conn = None
    cur = None

    current_month_year = datetime.now().strftime("%B_%Y")
    download_url = get_latest_dac_url()
    filename = os.path.basename(download_url)
    download_directory = os.path.join(os.path.dirname(__file__), 'data')
    csv_path = os.path.join(download_directory, filename)

    os.makedirs(download_directory, exist_ok=True)

    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST"),
            database=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD")
        )
        cur = conn.cursor()

        create_dac_tables(cur)
        conn.commit()

        cur.execute("SELECT 1 FROM cms_dac_import_log WHERE import_month = %s", (current_month_year,))
        if cur.fetchone():
            print(f"✅ DAC data for {current_month_year} already imported. Skipping.")
            return

        if not os.path.exists(csv_path):
            download_file(download_url, csv_path)

        with open(csv_path, newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                normalize_and_insert(cur, row)

        conn.commit()
        cur.execute("INSERT INTO cms_dac_import_log (import_month, file_name) VALUES (%s, %s)",
                    (current_month_year, filename))
        conn.commit()

    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
        cleanup_directory(download_directory)

if __name__ == "__main__":
    main()
