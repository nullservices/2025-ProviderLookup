# 🩺 CMS Healthcare Data Importers

This project contains importers that automatically download, normalize, and load the latest CMS datasets into PostgreSQL:

- **NPPES Importer** – National Plan and Provider Enumeration System  
- **CMS DAC Importer** – Doctors and Clinicians National Downloadable File

---

## ✅ Features

- Automatically downloads latest files directly from CMS  
- Skips already-imported months to prevent duplication  
- Normalizes raw CSVs into relational PostgreSQL tables  
- Logs each import in a monthly log table  
- Cleans up temporary files after execution  

---

## 📂 Tables Created

### NPPES Importer

- `providers`  
- `provider_addresses`  
- `provider_taxonomies`  
- `nppes_import_log`  

### CMS DAC Importer

- `cms_dac_clinicians`  
- `cms_dac_practice_locations`  
- `cms_dac_import_log`  

---

## 🚀 Usage

### Run the NPPES Importer

```bash
python nppes_importer.py
```

### Run the CMS DAC Importer

```bash
python cms_dac_importer.py
```

---

## ℹ️ Requirements

### Environment

- A PostgreSQL instance accessible to your script
- `.env` file with the following variables:

```env
DB_HOST=localhost
DB_NAME=your_database
DB_USER=your_username
DB_PASSWORD=your_password
```

### Python Dependencies

```bash
pip install psycopg2 requests tqdm python-dotenv
```

---

## 🔁 Automation

Each importer is designed to be run **monthly** (via cron, GitHub Actions, etc.).  
If the import for a given month is already present, it will automatically **skip** reprocessing that file.