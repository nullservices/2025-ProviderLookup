# 🩺 Healthcare Dataset Importers

This project contains importers that automatically download, normalize, and load the latest CMS and HHS OIG datasets into PostgreSQL:

- **NPPES Importer** – National Plan and Provider Enumeration System  
- **CMS DAC Importer** – Doctors and Clinicians National Downloadable File  
- **CMS Open Payments Importer** – General Payments, Research Payments, and Physician Ownership  
- **HHS OIG LEIE Importer** – List of Excluded Individuals/Entities

---

## ✅ Features

- Automatically downloads latest files directly from CMS or HHS  
- Skips already-imported months or years to prevent duplication  
- Normalizes raw CSVs into relational PostgreSQL tables  
- Logs each import in an audit table  
- Imports data in chunks to reduce memory usage  
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

### CMS Open Payments Importer

- `cms_open_payments_general_all`  
- `cms_open_payments_research_all`  
- `cms_open_payments_ownership_all`  
- `cms_open_payments_import_log`  

### HHS OIG LEIE Importer

- `hhs_leie_exclusions`  
- `hhs_leie_import_log`  

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

### Run the CMS Open Payments Importer

```bash
python cms_open_payments_importer.py
```

### Run the HHS LEIE Importer

```bash
python hhs_leie_importer.py
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

Each importer is designed to be run **monthly** (or yearly for Open Payments and LEIE).  
If the data for a given period has already been imported, the importer will automatically **skip** reprocessing that file.
