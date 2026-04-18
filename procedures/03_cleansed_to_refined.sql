USE dev_HealthConnect_raw;
GO

DROP PROCEDURE IF EXISTS Proc_HealthConnect_Cleansed_To_Refined;
GO

CREATE PROCEDURE Proc_HealthConnect_Cleansed_To_Refined
AS
BEGIN

    -- =========================
    -- PATIENTS
    -- =========================
    MERGE dev_HealthConnect_refined.dbo.refined_patients AS target
    USING (
        SELECT *
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY patient_id ORDER BY load_date DESC) rn
            FROM dev_HealthConnect_cleansed.dbo.cleansed_patients
        ) t WHERE rn = 1
    ) AS source
    ON target.patient_id = source.patient_id

    WHEN MATCHED THEN UPDATE SET
        first_name = source.first_name,
        last_name = source.last_name,
        gender = source.gender,
        date_of_birth = source.date_of_birth,
        state_code = source.state_code,
        city = source.city,
        phone = source.phone,
        load_date = source.load_date

    WHEN NOT MATCHED THEN INSERT VALUES (
        source.patient_id, source.first_name, source.last_name,
        source.gender, source.date_of_birth,
        source.state_code, source.city, source.phone, source.load_date
    );


    -- =========================
    -- PROVIDERS
    -- =========================
    MERGE dev_HealthConnect_refined.dbo.refined_providers AS target
    USING (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY provider_id ORDER BY load_date DESC) rn
        FROM dev_HealthConnect_cleansed.dbo.cleansed_providers
    ) source
    ON target.provider_id = source.provider_id

    WHEN MATCHED THEN UPDATE SET
        first_name = source.first_name,
        last_name = source.last_name,
        specialty = source.specialty,
        npi = source.npi,
        load_date = source.load_date

    WHEN NOT MATCHED THEN INSERT VALUES (
        source.provider_id, source.first_name, source.last_name,
        source.specialty, source.npi, source.load_date
    );


    -- =========================
    -- PAYERS
    -- =========================
    MERGE dev_HealthConnect_refined.dbo.refined_payers AS target
    USING (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY payer_id ORDER BY load_date DESC) rn
        FROM dev_HealthConnect_cleansed.dbo.cleansed_payers
    ) source
    ON target.payer_id = source.payer_id

    WHEN MATCHED THEN UPDATE SET
        payer_name = source.payer_name,
        load_date = source.load_date

    WHEN NOT MATCHED THEN INSERT VALUES (
        source.payer_id, source.payer_name, source.load_date
    );


    -- =========================
    -- ENCOUNTERS
    -- =========================
    MERGE dev_HealthConnect_refined.dbo.refined_encounters AS target
    USING (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY encounter_id ORDER BY load_date DESC) rn
        FROM dev_HealthConnect_cleansed.dbo.cleansed_encounters
    ) source
    ON target.encounter_id = source.encounter_id

    WHEN MATCHED THEN UPDATE SET
        patient_id = source.patient_id,
        provider_id = source.provider_id,
        encounter_type = source.encounter_type,
        encounter_start = source.encounter_start,
        encounter_end = source.encounter_end,
        height_cm = source.height_cm,
        weight_kg = source.weight_kg,
        systolic_bp = source.systolic_bp,
        diastolic_bp = source.diastolic_bp,
        load_date = source.load_date

    WHEN NOT MATCHED THEN INSERT VALUES (
        source.encounter_id, source.patient_id, source.provider_id,
        source.encounter_type, source.encounter_start,
        source.encounter_end, source.height_cm,
        source.weight_kg, source.systolic_bp,
        source.diastolic_bp, source.load_date
    );


    -- =========================
    -- CLAIMS
    -- =========================
    MERGE dev_HealthConnect_refined.dbo.refined_claims AS target
    USING (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY claim_id ORDER BY load_date DESC) rn
        FROM dev_HealthConnect_cleansed.dbo.cleansed_claims
    ) source
    ON target.claim_id = source.claim_id

    WHEN MATCHED THEN UPDATE SET
        encounter_id = source.encounter_id,
        payer_id = source.payer_id,
        admit_date = source.admit_date,
        discharge_date = source.discharge_date,
        total_billed_amount = source.total_billed_amount,
        total_allowed_amount = source.total_allowed_amount,
        total_paid_amount = source.total_paid_amount,
        claim_status = source.claim_status,
        load_date = source.load_date

    WHEN NOT MATCHED THEN INSERT VALUES (
        source.claim_id, source.encounter_id, source.payer_id,
        source.admit_date, source.discharge_date,
        source.total_billed_amount, source.total_allowed_amount,
        source.total_paid_amount, source.claim_status, source.load_date
    );

END;
GO

EXEC Proc_HealthConnect_Cleansed_To_Refined;