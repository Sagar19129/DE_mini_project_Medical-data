#  Medical Data Pipeline using Databricks (End-to-End Project)

---

## 🚀 Project Overview
This project implements a complete end-to-end data engineering pipeline using Databricks, following the Medallion Architecture (Bronze → Silver → Gold).

The pipeline ingests raw healthcare data from Google Drive using Fivetran, performs data cleaning and transformation, and generates business-ready KPIs and data cubes.

---

## 🏗️ Architecture Overview
---

##  Data Ingestion (Bronze Layer)

###  Source
- Data stored in Google Drive
- CSV files ingested using Fivetran

###  Process
- Data extracted from Google Drive container
- Loaded into Databricks as raw tables
- No transformations applied

---

##  Bronze Layer

- Stores raw ingested data
- Tables:
  - patients
  - encounters
  - procedures
  - organizations
  - payers

---

##  Silver Layer (Data Cleaning & Transformation)

###  Common Transformations
- Standardized column names (snake_case)
- Removed system columns (_line, _fivetran_synced)
- Trimmed whitespace
- Handled null values
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

Example:
duration_hours = (stop_date - start_date) in hours

---

###  Procedures Table
- Removed commas from base_cost
- Converted base_cost to DOUBLE
- Calculated procedure duration

Example:
procedure_duration = (stop - start) in hours

---

##  Gold Layer (Analytics Layer)

### 📌 Fact Tables
- fact_encounters
- fact_procedures

###  Dimension Tables
- dim_patients
- dim_payers
- dim_organizations
- dim_procedures
- dim_calendar

---

##  KPIs Implemented

### KPI 1: Encounter Mix
Distribution by encounter class

### KPI 2: Encounter Duration Split
Percentage of encounters > 24 hrs vs ≤ 24 hrs

### KPI 3: Zero Payer Coverage
Monthly payer coverage analysis

### KPI 4: Top Procedures
Ranked by average base cost

### KPI 5: Average Claim Cost
Average cost per payer

---

### KPI 6: 30-Day Readmission Rate

####  Objective
Measure percentage of patients readmitted within 30 days

####  Logic
- Use LAG() to find previous encounter
- Eligible encounter:
  - previous encounter exists
  - start_date >= previous_end_date
- Readmission:
  - within 30 days

####  Formula
Readmission Rate (%) = (Readmissions / Eligible Encounters) * 100

---

##  Data Cube

### Purpose
Enable multi-dimensional analysis

### Cubes Created
- Encounter Cube (time, payer, class)
- Procedure Cube (procedure, cost, frequency)

---

##  Orchestration

Implemented using Databricks Jobs

Pipeline Flow:
Bronze → Silver → Gold → KPIs → Data Cube

---

##  Technologies Used

- Databricks
- Apache Spark (PySpark + SQL)
- Delta Lake
- Fivetran
- GitHub

---

##  Data Flow Summary

Google Drive → Fivetran → Bronze → Silver → Gold → KPIs → Data Cube

---

##  Challenges & Solutions

Dirty Data → Fixed in Silver layer  
Incorrect KPIs → Fixed eligibility logic  
NULL values → Handled in aggregation  

---

##  Key Learnings

- Built Medallion Architecture
- Used window functions (LAG)
- Designed fact-dimension model
- Created KPIs and data cube
- Implemented orchestration

---

##  Conclusion

This project demonstrates a complete production-grade data pipeline from raw ingestion to business insights.
