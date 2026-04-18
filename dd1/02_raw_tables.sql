USE dev_HealthConnect_raw;

-- PATIENTS
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
CREATE TABLE raw_providers (
    provider_id INT,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    specialty VARCHAR(100),
    npi VARCHAR(50)
);

-- PAYERS
CREATE TABLE raw_payers (
    payer_id INT,
    payer_name VARCHAR(100)
);

-- ENCOUNTERS
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
CREATE TABLE raw_diagnoses (
    diagnosis_id INT,
    encounter_id INT,
    diagnosis_description VARCHAR(255),
    is_primary VARCHAR(10)
);

-- PROCEDURES
CREATE TABLE raw_procedures (
    procedure_id INT,
    encounter_id INT,
    procedure_description VARCHAR(255)
);

-- MEDICATIONS
CREATE TABLE raw_medications (
    medication_id INT,
    encounter_id INT,
    drug_name VARCHAR(100),
    route VARCHAR(50),
    dose VARCHAR(50),
    frequency VARCHAR(50),
    days_supply INT
);