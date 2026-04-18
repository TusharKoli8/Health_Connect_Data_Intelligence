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
        FORMAT='CSV',
        FIRSTROW=2,
        FIELDTERMINATOR=',',
        ROWTERMINATOR='0x0a'
    );

    -- PROVIDERS
    TRUNCATE TABLE raw_providers;

    BULK INSERT raw_providers
    FROM 'C:\SQL2025\HealthConnect-Data-Intelligence\data\providers_20250930.csv'
    WITH (
        FORMAT='CSV',
        FIRSTROW=2,
        FIELDTERMINATOR=',',
        ROWTERMINATOR='0x0a'
    );

    -- PAYERS
    TRUNCATE TABLE raw_payers;

    BULK INSERT raw_payers
    FROM 'C:\SQL2025\HealthConnect-Data-Intelligence\data\payers_20250930.csv'
    WITH (
        FORMAT='CSV',
        FIRSTROW=2,
        FIELDTERMINATOR=',',
        ROWTERMINATOR='0x0a'
    );

    -- ENCOUNTERS
    TRUNCATE TABLE raw_encounters;

    BULK INSERT raw_encounters
    FROM 'C:\SQL2025\HealthConnect-Data-Intelligence\data\encounters_20250930.csv'
    WITH (
        FORMAT='CSV',
        FIRSTROW=2,
        FIELDTERMINATOR=',',
        ROWTERMINATOR='0x0a'
    );

    -- CLAIMS
    TRUNCATE TABLE raw_claims;

    BULK INSERT raw_claims
    FROM 'C:\SQL2025\HealthConnect-Data-Intelligence\data\claims_20250930.csv'
    WITH (
        FORMAT='CSV',
        FIRSTROW=2,
        FIELDTERMINATOR=',',
        ROWTERMINATOR='0x0a'
    );

    -- DIAGNOSES
    TRUNCATE TABLE raw_diagnoses;

    BULK INSERT raw_diagnoses
    FROM 'C:\SQL2025\HealthConnect-Data-Intelligence\data\diagnoses_20250930.csv'
    WITH (
        FORMAT='CSV',
        FIRSTROW=2,
        FIELDTERMINATOR=',',
        ROWTERMINATOR='0x0a'
    );

    -- PROCEDURES
    TRUNCATE TABLE raw_procedures;

    BULK INSERT raw_procedures
    FROM 'C:\SQL2025\HealthConnect-Data-Intelligence\data\procedures_20250930.csv'
    WITH (
        FORMAT='CSV',
        FIRSTROW=2,
        FIELDTERMINATOR=',',
        ROWTERMINATOR='0x0a'
    );

    -- MEDICATIONS
    TRUNCATE TABLE raw_medications;

    BULK INSERT raw_medications
    FROM 'C:\SQL2025\HealthConnect-Data-Intelligence\data\medications_20250930.csv'
    WITH (
        FORMAT='CSV',
        FIRSTROW=2,
        FIELDTERMINATOR=',',
        ROWTERMINATOR='0x0a'
    );

END;
GO

EXEC Proc_HealthConnect_Source_To_Raw;