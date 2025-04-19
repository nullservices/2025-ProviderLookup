# 🩺 NPPES Importer

This module downloads, normalizes, and imports the latest CMS NPPES Data Dissemination File into PostgreSQL.

## ✅ Features

- Skips re-importing data if already logged for the month
- Normalizes data into clean, relational tables
- Tracks imports via `nppes_import_log`
- Cleans up downloaded files after processing

## 📂 Tables Created

- `providers`
- `provider_addresses`
- `provider_taxonomies`
- `nppes_import_log`

## 🚀 Usage

```bash
python nppes_importer.py
ℹ️ Requirements

Local PostgreSQL instance
Python packages: psycopg2, requests, tqdm

➡️ Script

nppes_importer.py
