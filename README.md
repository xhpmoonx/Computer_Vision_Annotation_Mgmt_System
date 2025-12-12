# Unified Computer Vision Database (CV-DBMS)

This repository contains a Python-based ETL pipeline for constructing a unified
SQLite database from three major computer vision datasets:
MS COCO, PASCAL VOC, and OpenImages.

## Contents
- `schema/schema.sql`: Unified relational schema
- `src/`: Dataset-specific ETL scripts

## How to Build the Database
1. Download COCO, VOC, and OpenImages annotations separately.
2. Place dataset files in the expected directories (see comments in ETL scripts).
3. Run:
   ```bash
   python src/init_db.py
   python src/import_coco.py
   python src/import_voc.py
   python src/import_openimages.py
   ```
