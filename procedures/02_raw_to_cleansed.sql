USE dev_HealthConnect_raw;
GO

DROP PROCEDURE IF EXISTS Proc_HealthConnect_Raw_To_Cleansed;
GO

CREATE PROCEDURE Proc_HealthConnect_Raw_To_Cleansed
AS
BEGIN

    -- =========================
    -- PATIENTS
    -- =========================
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
    FROM dev_HealthConnect_raw.dbo.raw_patients;


    -- =========================
    -- PROVIDERS
    -- =========================
    TRUNCATE TABLE dev_HealthConnect_cleansed.dbo.cleansed_providers;

    INSERT INTO dev_HealthConnect_cleansed.dbo.cleansed_providers
    SELECT
        provider_id,
        UPPER(LTRIM(RTRIM(first_name))),
        UPPER(LTRIM(RTRIM(last_name))),
        UPPER(LTRIM(RTRIM(specialty))),
        npi,
        GETDATE()
    FROM dev_HealthConnect_raw.dbo.raw_providers;


    -- =========================
    -- PAYERS
    -- =========================
    TRUNCATE TABLE dev_HealthConnect_cleansed.dbo.cleansed_payers;

    INSERT INTO dev_HealthConnect_cleansed.dbo.cleansed_payers
    SELECT
        payer_id,
        UPPER(LTRIM(RTRIM(payer_name))),
        GETDATE()
    FROM dev_HealthConnect_raw.dbo.raw_payers;


    -- =========================
    -- ENCOUNTERS
    -- =========================
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
    FROM dev_HealthConnect_raw.dbo.raw_encounters;


    -- =========================
    -- CLAIMS
    -- =========================
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
    FROM dev_HealthConnect_raw.dbo.raw_claims;


    -- =========================
    -- DIAGNOSES
    -- =========================
    TRUNCATE TABLE dev_HealthConnect_cleansed.dbo.cleansed_diagnoses;

    INSERT INTO dev_HealthConnect_cleansed.dbo.cleansed_diagnoses
    SELECT
        diagnosis_id,
        patient_id,
        encounter_id,
        UPPER(LTRIM(RTRIM(diagnosis_code))),
        UPPER(LTRIM(RTRIM(diagnosis_description))),
        GETDATE()
    FROM dev_HealthConnect_raw.dbo.raw_diagnoses;


    -- =========================
    -- PROCEDURES
    -- =========================
    TRUNCATE TABLE dev_HealthConnect_cleansed.dbo.cleansed_procedures;

    INSERT INTO dev_HealthConnect_cleansed.dbo.cleansed_procedures
    SELECT
        procedure_id,
        patient_id,
        encounter_id,
        UPPER(LTRIM(RTRIM(procedure_code))),
        UPPER(LTRIM(RTRIM(procedure_description))),
        GETDATE()
    FROM dev_HealthConnect_raw.dbo.raw_procedures;


    -- =========================
    -- MEDICATIONS
    -- =========================
    TRUNCATE TABLE dev_HealthConnect_cleansed.dbo.cleansed_medications;

    INSERT INTO dev_HealthConnect_cleansed.dbo.cleansed_medications
    SELECT
        medication_id,
        patient_id,
        encounter_id,
        UPPER(LTRIM(RTRIM(medication_name))),
        dosage,
        GETDATE()
    FROM dev_HealthConnect_raw.dbo.raw_medications;

END;
GO

EXEC Proc_HealthConnect_Raw_To_Cleansed;