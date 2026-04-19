-- ========================================
-- PROJECT: HealthConnect Data Intelligence
-- AUTHOR: Tushar Koli
-- =========================================

-- DESCRIPTION:
-- This project implements an end to end ETL pipeline using SQL Server.
-- It loads healthcare data from CSV files into RAW, CLEANSED, and REFINED layers.
-- The project also generates KPI reports for analysis.

-- OBJECTIVE:
-- 1. Load CSV data into SQL Server
-- 2. Perform data cleaning and transformation
-- 3. Implement SCD Type 1
-- 4. Generate KPI reports

-- ARCHITECTURE:
-- CSV : RAW : CLEANSED : REFINED : KPI

-- HOW TO RUN:
-- 1. Update file paths
-- 2. Run full script
-- 3. Execute master pipeline
-- ===========================================

-- =====================
-- 1. DATABASE CREATION
-- =====================

IF DB_ID('dev_HealthConnect_raw') IS NULL
    CREATE DATABASE dev_HealthConnect_raw;
GO

IF DB_ID('dev_HealthConnect_cleansed') IS NULL
    CREATE DATABASE dev_HealthConnect_cleansed;
GO

IF DB_ID('dev_HealthConnect_refined') IS NULL
    CREATE DATABASE dev_HealthConnect_refined;
GO

-- This step creates three separate databases to implement a layered architecture:
-- RAW for initial data loading, CLEANSED for transformed data:
-- REFINED for final reporting ready data.


-- ================================
-- RAW TABLES (Stores raw CSV data)
-- ================================

USE dev_HealthConnect_raw;
GO

-- PATIENTS
IF OBJECT_ID('raw_patients', 'U') IS NOT NULL DROP TABLE raw_patients;
CREATE TABLE raw_patients (
    patient_id INT,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    gender VARCHAR(10),
    date_of_birth DATE,
    state_code VARCHAR(10),
    city VARCHAR(50),
    phone VARCHAR(20)
);

-- PROVIDERS
IF OBJECT_ID('raw_providers', 'U') IS NOT NULL DROP TABLE raw_providers;
CREATE TABLE raw_providers (
    provider_id INT,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    specialty VARCHAR(100),
    npi VARCHAR(50)
);

-- PAYERS
IF OBJECT_ID('raw_payers', 'U') IS NOT NULL DROP TABLE raw_payers;
CREATE TABLE raw_payers (
    payer_id INT,
    payer_name VARCHAR(100)
);

-- ENCOUNTERS
IF OBJECT_ID('raw_encounters', 'U') IS NOT NULL DROP TABLE raw_encounters;
CREATE TABLE raw_encounters (
    encounter_id INT,
    patient_id INT,
    provider_id INT,
    encounter_type VARCHAR(50),
    encounter_start DATETIME,
    encounter_end DATETIME,
    height_cm DECIMAL(5,2),
    weight_kg DECIMAL(5,2),
    systolic_bp INT,
    diastolic_bp INT
);

-- CLAIMS
IF OBJECT_ID('raw_claims', 'U') IS NOT NULL DROP TABLE raw_claims;
CREATE TABLE raw_claims (
    claim_id INT,
    encounter_id INT,
    payer_id INT,
    admit_date DATE,
    discharge_date DATE,
    total_billed_amount DECIMAL(12,2),
    total_allowed_amount DECIMAL(12,2),
    total_paid_amount DECIMAL(12,2),
    claim_status VARCHAR(50)
);

-- DIAGNOSES
IF OBJECT_ID('raw_diagnoses', 'U') IS NOT NULL DROP TABLE raw_diagnoses;
CREATE TABLE raw_diagnoses (
    diagnosis_id INT,
    patient_id INT,
    diagnosis_code VARCHAR(50),
    diagnosis_description VARCHAR(255)
);

-- PROCEDURES
IF OBJECT_ID('raw_procedures', 'U') IS NOT NULL DROP TABLE raw_procedures;
CREATE TABLE raw_procedures (
    procedure_id INT,
    patient_id INT,
    procedure_code VARCHAR(50),
    procedure_description VARCHAR(255)
);

-- MEDICATIONS
IF OBJECT_ID('raw_medications', 'U') IS NOT NULL DROP TABLE raw_medications;
CREATE TABLE raw_medications (
    medication_id INT,
    patient_id INT,
    medication_name VARCHAR(100),
    dosage VARCHAR(50)
);

-- In this step, raw tables are created to store data exactly as received from CSV files.
-- No transformations are applied at this stage to preserve original data integrity.

-- ===================
-- CLEANSED TABLES
-- Data cleaning layer
-- ===================

USE dev_HealthConnect_cleansed;
GO

-- PATIENTS
IF OBJECT_ID('cleansed_patients', 'U') IS NOT NULL DROP TABLE cleansed_patients;
CREATE TABLE cleansed_patients (
    patient_id INT,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    gender VARCHAR(10),
    date_of_birth DATE,
    state_code VARCHAR(10),
    city VARCHAR(50),
    phone VARCHAR(20),
    load_date DATETIME
);

-- PROVIDERS
IF OBJECT_ID('cleansed_providers', 'U') IS NOT NULL DROP TABLE cleansed_providers;
CREATE TABLE cleansed_providers (
    provider_id INT,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    specialty VARCHAR(100),
    npi VARCHAR(50),
    load_date DATETIME
);

-- PAYERS
IF OBJECT_ID('cleansed_payers', 'U') IS NOT NULL DROP TABLE cleansed_payers;
CREATE TABLE cleansed_payers (
    payer_id INT,
    payer_name VARCHAR(100),
    load_date DATETIME
);

-- ENCOUNTERS
IF OBJECT_ID('cleansed_encounters', 'U') IS NOT NULL DROP TABLE cleansed_encounters;
CREATE TABLE cleansed_encounters (
    encounter_id INT,
    patient_id INT,
    provider_id INT,
    encounter_type VARCHAR(50),
    encounter_start DATETIME,
    encounter_end DATETIME,
    height_cm DECIMAL(5,2),
    weight_kg DECIMAL(5,2),
    systolic_bp INT,
    diastolic_bp INT,
    load_date DATETIME
);

-- CLAIMS
IF OBJECT_ID('cleansed_claims', 'U') IS NOT NULL DROP TABLE cleansed_claims;
CREATE TABLE cleansed_claims (
    claim_id INT,
    encounter_id INT,
    payer_id INT,
    admit_date DATE,
    discharge_date DATE,
    total_billed_amount DECIMAL(12,2),
    total_allowed_amount DECIMAL(12,2),
    total_paid_amount DECIMAL(12,2),
    claim_status VARCHAR(50),
    load_date DATETIME
);

-- DIAGNOSES
IF OBJECT_ID('cleansed_diagnoses', 'U') IS NOT NULL DROP TABLE cleansed_diagnoses;
CREATE TABLE cleansed_diagnoses (
    diagnosis_id INT,
    patient_id INT,
    diagnosis_code VARCHAR(50),
    diagnosis_description VARCHAR(255),
    load_date DATETIME
);

-- PROCEDURES
IF OBJECT_ID('cleansed_procedures', 'U') IS NOT NULL DROP TABLE cleansed_procedures;
CREATE TABLE cleansed_procedures (
    procedure_id INT,
    patient_id INT,
    procedure_code VARCHAR(50),
    procedure_description VARCHAR(255),
    load_date DATETIME
);

-- MEDICATIONS
IF OBJECT_ID('cleansed_medications', 'U') IS NOT NULL DROP TABLE cleansed_medications;
CREATE TABLE cleansed_medications (
    medication_id INT,
    patient_id INT,
    medication_name VARCHAR(100),
    dosage VARCHAR(50),
    load_date DATETIME
);

-- Cleansed tables are created to store processed data.
-- Basic data cleaning such as trimming spaces, standardizing text and adding load timestamps will be applied in this layer.

-- ===========================
-- SOURCE TO RAW (BULK INSERT)
-- ===========================

USE dev_HealthConnect_raw;
GO

DROP PROCEDURE IF EXISTS Proc_HealthConnect_Source_To_Raw;
GO

CREATE PROCEDURE Proc_HealthConnect_Source_To_Raw
AS
BEGIN

    -- PATIENTS
    TRUNCATE TABLE raw_patients;
    BULK INSERT raw_patients
    FROM 'C:\SQL2025\HealthConnect-Data-Intelligence\data\patients_20250930.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '0x0a'
    );

    -- PROVIDERS
    TRUNCATE TABLE raw_providers;
    BULK INSERT raw_providers
    FROM 'C:\SQL2025\HealthConnect-Data-Intelligence\data\providers_20250930.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '0x0a'
    );

    -- PAYERS
    TRUNCATE TABLE raw_payers;
    BULK INSERT raw_payers
    FROM 'C:\SQL2025\HealthConnect-Data-Intelligence\data\payers_20250930.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '0x0a'
    );

    -- ENCOUNTERS
    TRUNCATE TABLE raw_encounters;
    BULK INSERT raw_encounters
    FROM 'C:\SQL2025\HealthConnect-Data-Intelligence\data\encounters_20250930.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '0x0a'
    );

    -- CLAIMS
    TRUNCATE TABLE raw_claims;
    BULK INSERT raw_claims
    FROM 'C:\SQL2025\HealthConnect-Data-Intelligence\data\claims_20250930.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '0x0a'
    );

    -- DIAGNOSES
    TRUNCATE TABLE raw_diagnoses;
    BULK INSERT raw_diagnoses
    FROM 'C:\SQL2025\HealthConnect-Data-Intelligence\data\diagnoses_20250930.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '0x0a'
    );

    -- PROCEDURES
    TRUNCATE TABLE raw_procedures;
    BULK INSERT raw_procedures
    FROM 'C:\SQL2025\HealthConnect-Data-Intelligence\data\procedures_20250930.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '0x0a'
    );

    -- MEDICATIONS
    TRUNCATE TABLE raw_medications;
    BULK INSERT raw_medications
    FROM 'C:\SQL2025\HealthConnect-Data-Intelligence\data\medications_20250930.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '0x0a'
    );

END;
GO


EXEC Proc_HealthConnect_Source_To_Raw;

-- This step loads data from CSV files into raw tables using BULK INSERT.
-- Existing data is truncated to ensure fresh data loading for each run.

-- ===========================
-- REFINED TABLES (Final Layer)
-- ============================

USE dev_HealthConnect_refined;
GO

-- PATIENTS
IF OBJECT_ID('refined_patients', 'U') IS NOT NULL DROP TABLE refined_patients;
CREATE TABLE refined_patients (
    patient_id INT PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    gender VARCHAR(10),
    date_of_birth DATE,
    state_code VARCHAR(10),
    city VARCHAR(50),
    phone VARCHAR(20),
    load_date DATETIME
);

-- PROVIDERS
IF OBJECT_ID('refined_providers', 'U') IS NOT NULL DROP TABLE refined_providers;
CREATE TABLE refined_providers (
    provider_id INT PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    specialty VARCHAR(100),
    npi VARCHAR(50),
    load_date DATETIME
);

-- PAYERS
IF OBJECT_ID('refined_payers', 'U') IS NOT NULL DROP TABLE refined_payers;
CREATE TABLE refined_payers (
    payer_id INT PRIMARY KEY,
    payer_name VARCHAR(100),
    load_date DATETIME
);

-- ENCOUNTERS
IF OBJECT_ID('refined_encounters', 'U') IS NOT NULL DROP TABLE refined_encounters;
CREATE TABLE refined_encounters (
    encounter_id INT PRIMARY KEY,
    patient_id INT,
    provider_id INT,
    encounter_type VARCHAR(50),
    encounter_start DATETIME,
    encounter_end DATETIME,
    height_cm DECIMAL(5,2),
    weight_kg DECIMAL(5,2),
    systolic_bp INT,
    diastolic_bp INT,
    load_date DATETIME
);

-- CLAIMS
IF OBJECT_ID('refined_claims', 'U') IS NOT NULL DROP TABLE refined_claims;
CREATE TABLE refined_claims (
    claim_id INT PRIMARY KEY,
    encounter_id INT,
    payer_id INT,
    admit_date DATE,
    discharge_date DATE,
    total_billed_amount DECIMAL(12,2),
    total_allowed_amount DECIMAL(12,2),
    total_paid_amount DECIMAL(12,2),
    claim_status VARCHAR(50),
    load_date DATETIME
);

-- DIAGNOSES
IF OBJECT_ID('refined_diagnoses', 'U') IS NOT NULL DROP TABLE refined_diagnoses;
CREATE TABLE refined_diagnoses (
    diagnosis_id INT PRIMARY KEY,
    patient_id INT,
    diagnosis_code VARCHAR(50),
    diagnosis_description VARCHAR(255),
    load_date DATETIME
);

-- PROCEDURES
IF OBJECT_ID('refined_procedures', 'U') IS NOT NULL DROP TABLE refined_procedures;
CREATE TABLE refined_procedures (
    procedure_id INT PRIMARY KEY,
    patient_id INT,
    procedure_code VARCHAR(50),
    procedure_description VARCHAR(255),
    load_date DATETIME
);

-- MEDICATIONS
IF OBJECT_ID('refined_medications', 'U') IS NOT NULL DROP TABLE refined_medications;
CREATE TABLE refined_medications (
    medication_id INT PRIMARY KEY,
    patient_id INT,
    medication_name VARCHAR(100),
    dosage VARCHAR(50),
    load_date DATETIME
);

-- Refined tables represent the final structured data used for reporting.
-- Primary keys are applied and data is maintained using SCD Type 1 logic.

-- =====================================
-- RAW TO CLEANSED (DATA TRANSFORMATION)
-- =====================================

USE dev_HealthConnect_raw;
GO

DROP PROCEDURE IF EXISTS Proc_HealthConnect_Raw_To_Cleansed;
GO

CREATE PROCEDURE Proc_HealthConnect_Raw_To_Cleansed
AS
BEGIN

    -- PATIENTS
    TRUNCATE TABLE dev_HealthConnect_cleansed.dbo.cleansed_patients;

    INSERT INTO dev_HealthConnect_cleansed.dbo.cleansed_patients
    SELECT
        patient_id,
        UPPER(LTRIM(RTRIM(first_name))),
        UPPER(LTRIM(RTRIM(last_name))),
        UPPER(LTRIM(RTRIM(gender))),
        date_of_birth,
        UPPER(LTRIM(RTRIM(state_code))),
        UPPER(LTRIM(RTRIM(city))),
        phone,
        GETDATE()
    FROM raw_patients;

    -- PROVIDERS
    TRUNCATE TABLE dev_HealthConnect_cleansed.dbo.cleansed_providers;

    INSERT INTO dev_HealthConnect_cleansed.dbo.cleansed_providers
    SELECT
        provider_id,
        UPPER(LTRIM(RTRIM(first_name))),
        UPPER(LTRIM(RTRIM(last_name))),
        UPPER(LTRIM(RTRIM(specialty))),
        npi,
        GETDATE()
    FROM raw_providers;

    -- PAYERS
    TRUNCATE TABLE dev_HealthConnect_cleansed.dbo.cleansed_payers;

    INSERT INTO dev_HealthConnect_cleansed.dbo.cleansed_payers
    SELECT
        payer_id,
        UPPER(LTRIM(RTRIM(payer_name))),
        GETDATE()
    FROM raw_payers;

    -- ENCOUNTERS
    TRUNCATE TABLE dev_HealthConnect_cleansed.dbo.cleansed_encounters;

    INSERT INTO dev_HealthConnect_cleansed.dbo.cleansed_encounters
    SELECT
        encounter_id,
        patient_id,
        provider_id,
        UPPER(LTRIM(RTRIM(encounter_type))),
        encounter_start,
        encounter_end,
        height_cm,
        weight_kg,
        systolic_bp,
        diastolic_bp,
        GETDATE()
    FROM raw_encounters;

    -- CLAIMS
    TRUNCATE TABLE dev_HealthConnect_cleansed.dbo.cleansed_claims;

    INSERT INTO dev_HealthConnect_cleansed.dbo.cleansed_claims
    SELECT
        claim_id,
        encounter_id,
        payer_id,
        admit_date,
        discharge_date,
        total_billed_amount,
        total_allowed_amount,
        total_paid_amount,
        UPPER(LTRIM(RTRIM(claim_status))),
        GETDATE()
    FROM raw_claims;

    -- DIAGNOSES
    TRUNCATE TABLE dev_HealthConnect_cleansed.dbo.cleansed_diagnoses;

    INSERT INTO dev_HealthConnect_cleansed.dbo.cleansed_diagnoses
    SELECT
        diagnosis_id,
        patient_id,
        UPPER(LTRIM(RTRIM(diagnosis_code))),
        UPPER(LTRIM(RTRIM(diagnosis_description))),
        GETDATE()
    FROM raw_diagnoses;

    -- PROCEDURES
    TRUNCATE TABLE dev_HealthConnect_cleansed.dbo.cleansed_procedures;

    INSERT INTO dev_HealthConnect_cleansed.dbo.cleansed_procedures
    SELECT
        procedure_id,
        patient_id,
        UPPER(LTRIM(RTRIM(procedure_code))),
        UPPER(LTRIM(RTRIM(procedure_description))),
        GETDATE()
    FROM raw_procedures;

    -- MEDICATIONS
    TRUNCATE TABLE dev_HealthConnect_cleansed.dbo.cleansed_medications;

    INSERT INTO dev_HealthConnect_cleansed.dbo.cleansed_medications
    SELECT
        medication_id,
        patient_id,
        UPPER(LTRIM(RTRIM(medication_name))),
        dosage,
        GETDATE()
    FROM raw_medications;

END;
GO

EXEC Proc_HealthConnect_Raw_To_Cleansed;

-- Refined tables represent the final structured data used for reporting.
-- Primary keys are applied and data is maintained using SCD Type 1 logic.

-- ================================
-- CLEANSED TO REFINED (SCD TYPE 1)
-- ================================

USE dev_HealthConnect_refined;
GO

DROP PROCEDURE IF EXISTS Proc_HealthConnect_Cleansed_To_Refined;
GO

CREATE PROCEDURE Proc_HealthConnect_Cleansed_To_Refined
AS
BEGIN

    -- PATIENTS
    MERGE refined_patients AS target
    USING dev_HealthConnect_cleansed.dbo.cleansed_patients AS source
    ON target.patient_id = source.patient_id

    WHEN MATCHED THEN UPDATE SET
        target.first_name = source.first_name,
        target.last_name = source.last_name,
        target.gender = source.gender,
        target.date_of_birth = source.date_of_birth,
        target.state_code = source.state_code,
        target.city = source.city,
        target.phone = source.phone,
        target.load_date = source.load_date

    WHEN NOT MATCHED THEN
        INSERT (
            patient_id, first_name, last_name, gender,
            date_of_birth, state_code, city, phone, load_date
        )
        VALUES (
            source.patient_id, source.first_name, source.last_name,
            source.gender, source.date_of_birth,
            source.state_code, source.city, source.phone, source.load_date
        );

    -- PROVIDERS
    MERGE refined_providers AS target
    USING dev_HealthConnect_cleansed.dbo.cleansed_providers AS source
    ON target.provider_id = source.provider_id

    WHEN MATCHED THEN UPDATE SET
        target.first_name = source.first_name,
        target.last_name = source.last_name,
        target.specialty = source.specialty,
        target.npi = source.npi,
        target.load_date = source.load_date

    WHEN NOT MATCHED THEN
        INSERT VALUES (
            source.provider_id, source.first_name, source.last_name,
            source.specialty, source.npi, source.load_date
        );

    -- PAYERS
    MERGE refined_payers AS target
    USING dev_HealthConnect_cleansed.dbo.cleansed_payers AS source
    ON target.payer_id = source.payer_id

    WHEN MATCHED THEN UPDATE SET
        target.payer_name = source.payer_name,
        target.load_date = source.load_date

    WHEN NOT MATCHED THEN
        INSERT VALUES (
            source.payer_id, source.payer_name, source.load_date
        );

    -- CLAIMS
    MERGE refined_claims AS target
    USING dev_HealthConnect_cleansed.dbo.cleansed_claims AS source
    ON target.claim_id = source.claim_id

    WHEN MATCHED THEN UPDATE SET
        target.encounter_id = source.encounter_id,
        target.payer_id = source.payer_id,
        target.admit_date = source.admit_date,
        target.discharge_date = source.discharge_date,
        target.total_billed_amount = source.total_billed_amount,
        target.total_allowed_amount = source.total_allowed_amount,
        target.total_paid_amount = source.total_paid_amount,
        target.claim_status = source.claim_status,
        target.load_date = source.load_date

    WHEN NOT MATCHED THEN
        INSERT VALUES (
            source.claim_id, source.encounter_id, source.payer_id,
            source.admit_date, source.discharge_date,
            source.total_billed_amount, source.total_allowed_amount,
            source.total_paid_amount, source.claim_status, source.load_date
        );

END;
GO

EXEC Proc_HealthConnect_Cleansed_To_Refined;

-- This step applies SCD Type 1 logic using MERGE.
-- Existing records are updated and new records are inserted ensuring no duplicate entries in the refined layer.

-- =========================
-- MASTER PIPELINE
-- Executes full ETL process
-- =========================

USE dev_HealthConnect_raw;
GO

DROP PROCEDURE IF EXISTS Proc_HealthConnect_Master_Pipeline;
GO

CREATE PROCEDURE Proc_HealthConnect_Master_Pipeline
AS
BEGIN

    EXEC Proc_HealthConnect_Source_To_Raw;

    EXEC Proc_HealthConnect_Raw_To_Cleansed;

    EXEC dev_HealthConnect_refined.dbo.Proc_HealthConnect_Cleansed_To_Refined;

END;
GO


EXEC Proc_HealthConnect_Master_Pipeline;

-- The master pipeline procedure orchestrates the entire ETL process.
-- It sequentially executes source to raw, raw to cleansed,and cleansed to refined steps in a single run.

-- =========================================
-- KPI VIEWS (REPORTING LAYER)
-- =========================================

USE dev_HealthConnect_refined;
GO

-- 1. PATIENT SUMMARY BY STATE
IF OBJECT_ID('vw_patient_summary', 'V') IS NOT NULL DROP VIEW vw_patient_summary;
GO
CREATE VIEW vw_patient_summary AS
SELECT 
    state_code,
    COUNT(*) AS total_patients
FROM refined_patients
GROUP BY state_code;
GO


-- 2. GENDER DISTRIBUTION
IF OBJECT_ID('vw_gender_distribution', 'V') IS NOT NULL DROP VIEW vw_gender_distribution;
GO
CREATE VIEW vw_gender_distribution AS
SELECT 
    gender,
    COUNT(*) AS total_patients
FROM refined_patients
GROUP BY gender;
GO


-- 3. CLAIMS SUMMARY
IF OBJECT_ID('vw_claims_summary', 'V') IS NOT NULL DROP VIEW vw_claims_summary;
GO
CREATE VIEW vw_claims_summary AS
SELECT 
    claim_status,
    COUNT(*) AS total_claims,
    SUM(total_paid_amount) AS total_revenue
FROM refined_claims
GROUP BY claim_status;
GO


-- 4. REVENUE BY PAYER
IF OBJECT_ID('vw_revenue_by_payer', 'V') IS NOT NULL DROP VIEW vw_revenue_by_payer;
GO
CREATE VIEW vw_revenue_by_payer AS
SELECT 
    payer_id,
    SUM(total_paid_amount) AS total_revenue
FROM refined_claims
GROUP BY payer_id;
GO

--Test

SELECT * FROM vw_patient_summary;
SELECT * FROM vw_gender_distribution;
SELECT * FROM vw_claims_summary;
SELECT * FROM vw_revenue_by_payer;

-- KPI views are created for reporting and analysis.
-- These views provide insights such as patient distribution,claims summary, and revenue metrics.

-- =========================================
-- FINAL EXECUTION
-- =========================================

EXEC Proc_HealthConnect_Master_Pipeline;

-- This step executes the complete ETL pipeline and retrieves final results from KPI views for validation and analysis.


-- VIEW RESULTS
SELECT * FROM vw_patient_summary;
SELECT * FROM vw_claims_summary;


-- ==================
-- PROJECT COMPLETION
-- ==================
-- This project successfully implements an end-to-end ETL pipeline using SQL Server.

-- Data is loaded from CSV files into RAW tables,transformed in the CLEANSED layer, and stored in REFINED tables for reporting purposes.

-- The pipeline is automated using stored procedures and a master procedure, ensuring efficient and repeatable data processing.

-- KPI views are created on top of the refined layer to provide meaningful insights such as patient distribution and revenue analysis.

-- This solution demonstrates practical implementation of data engineering concepts including ETL processing, data cleaning.

-- ==============
-- END OF PROJECT
-- ==============

--Thank You