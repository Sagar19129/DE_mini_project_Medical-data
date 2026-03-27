# Medical Data Pipeline using Databricks (End-to-End Project)

---

##  Project Overview
This project implements a complete end-to-end data engineering pipeline using Databricks, following the Medallion Architecture (Bronze → Silver → Gold).

The pipeline ingests raw healthcare data, performs cleaning and transformation, and generates business-ready KPIs and data cubes.

---

##  Architecture Overview

---

##  Data Ingestion (Bronze Layer)

###  Source
- Data stored in Google Drive
- CSV files ingested using Fivetran

###  Process
- Data extracted from Google Drive container
- Loaded into Databricks as raw tables
- No transformations applied

###  Purpose
- Maintain raw source of truth
- Enable reprocessing

---

##  Bronze Layer

- Stores raw ingested data
- Tables include:
  - patients
  - encounters
  - procedures
  - organizations
  - payers

No transformations applied in this layer

---

##  Silver Layer (Data Cleaning & Transformation)

### 🔹 Common Transformations
- Standardized column names (snake_case)
- Removed system columns (_line, _fivetran_synced)
- Trimmed whitespace
- Handled null and unknown values
- Fixed data types
- Converted timestamps

---

###  Patients Table
- Removed numeric values from:
  - first_name
  - last_name
  - maiden_name
- Cleaned birth_place column

---

###  Encounters Table
- Converted start_date and stop_date to timestamp
- Calculated encounter duration in hours

---

###  Procedures Table
- Removed commas from base_cost
- Converted base_cost to DOUBLE
- Calculated procedure duration
- Standardized codes and descriptions

---

##  Gold Layer (Analytics Layer)

###  Fact Tables

#### fact_encounters
- Contains encounter-level data
- Includes:
  - patient_id
  - payer_id
  - duration
  - timestamps

#### fact_procedures
- Contains procedure-level data
- Includes:
  - cost
  - duration
  - encounter linkage

---

###  Dimension Tables
- dim_patients
- dim_payers
- dim_organizations
- dim_procedures
- dim_calendar

---

##  KPIs Implemented

### KPI 1: Encounter Mix
- Distribution by encounter class (quarterly)

### KPI 2: Encounter Duration Split
- % of encounters > 24 hrs vs ≤ 24 hrs (monthly)

### KPI 3: Zero Payer Coverage
- Monthly payer coverage analysis

### KPI 4: Top Procedures
- Ranked by average base cost (half-yearly)

### KPI 5: Avg Claim Cost
- Average cost by payer

### KPI 6: 30-Day Readmission Rate

#### Logic:
- Used LAG() to find previous encounter
- Eligible encounter:
  - Previous encounter exists
  - Start ≥ previous end
- Readmission:
  - Within 30 days

#### Formula:

---

##  Data Cube

###  Purpose
Enable multi-dimensional analysis

###  Cubes Created

#### Encounter Cube
- Dimensions:
  - time
  - payer
  - encounter class

#### Procedure Cube
- Dimensions:
  - procedure
  - cost
  - frequency

---

##  Orchestration

- Implemented using Databricks Jobs

### Pipeline Flow:
---

##  Challenges & Solutions

### Dirty Data
 Fixed in Silver layer

### Incorrect KPI values (>100%)
 Fixed eligibility logic

### Missing months / NULL values
 Handled using filtering and logic fixes

---

##  Key Learnings

- Built Medallion Architecture pipeline
- Used Spark SQL and window functions
- Designed fact-dimension model
- Created KPIs and data cube
- Implemented orchestration

---

##  Conclusion

This project demonstrates a complete production-style data pipeline from raw data ingestion to business insights using modern data engineering practices.