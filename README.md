# 2025-ProviderLookup
 
🩺 NPPES Importer
This module downloads, normalizes, and ingests the latest CMS NPPES Data Dissemination File into a PostgreSQL database.

Automatically skips re-importing already processed months.

Normalizes providers, practice locations, and taxonomies into relational tables.

Logs import metadata in nppes_import_log to prevent duplicates.

📦 Source: CMS NPPES Monthly ZIP

Tables Created:

providers

provider_addresses

provider_taxonomies

nppes_import_log

➡️ Script: nppes_importer.py

