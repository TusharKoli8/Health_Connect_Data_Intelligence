#  HealthConnect Data Intelligence

##  Project Overview

HealthConnect Data Intelligence is an end-to-end SQL-based data engineering project designed to process healthcare data from multiple CSV source files into a structured data warehouse.

The project implements a **3-layer architecture (RAW → CLEANSED → REFINED)** and enables analytics through KPI-driven reporting.

Database Structure

###  RAW Layer (`dev_HealthConnect_raw`)
* **Load Type:** Truncate & Load.
* **Purpose:** Direct ingestion of CSV data via `BULK INSERT`.

###  CLEANSED Layer (`dev_HealthConnect_cleansed`)
* **Transformations:** Trim spaces, convert to uppercase, and add `load_date`.
* **Purpose:** Standardized staging area for data quality.

###  REFINED Layer (`dev_HealthConnect_refined`)
* **Logic:** Implements **Slowly Changing Dimension (Type 1)**.
* **Purpose:** Final reporting layer with deduplicated, latest records.


##  Data Sources
Processes core healthcare domains: **Patients, Providers, Payers, Encounters, Claims, Diagnoses, Procedures, and Medications.**


##  ETL Pipeline
1. **Source → RAW:** Automated bulk ingestion.
2. **RAW → CLEANSED:** Multi-step cleaning and standardization scripts.
3. **CLEANSED → REFINED:** Advanced `MERGE` logic for upserts and SCD.
4. **Master Pipeline:** Single-point execution via `Proc_HealthConnect_Master_Pipeline`.


##  KPI & Reporting Views
* **Patient Summary:** Regional population analysis.
* **Gender Distribution:** Demographic breakdown.
* **Claims Summary:** Lifecycle tracking of insurance claims.
* **Revenue Summary:** Financial performance and billed-vs-paid gap analysis.


##  Technologies Used
* **Database:** SQL Server 2022
* **Language:** T-SQL (Stored Procedures, Views, MERGE)
* **Tools:** SSMS (SQL Server Management Studio)


##  How to Run
1. Create environment databases: `raw`, `cleansed`, and `refined`.
2. Execute DDL scripts to initialize tables.
3. Trigger the end-to-end workflow:
   sql
   EXEC Proc_HealthConnect_Master_Pipeline;