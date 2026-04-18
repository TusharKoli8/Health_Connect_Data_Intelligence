USE dev_HealthConnect_cleansed;

-- PATIENTS
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
CREATE TABLE cleansed_providers (
    provider_id INT,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    specialty VARCHAR(100),
    npi VARCHAR(50),
    load_date DATETIME
);

-- PAYERS
CREATE TABLE cleansed_payers (
    payer_id INT,
    payer_name VARCHAR(100),
    load_date DATETIME
);

-- ENCOUNTERS
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
CREATE TABLE cleansed_diagnoses (
    diagnosis_id INT,
    encounter_id INT,
    diagnosis_description VARCHAR(255),
    is_primary VARCHAR(10),
    load_date DATETIME
);

-- PROCEDURES
CREATE TABLE cleansed_procedures (
    procedure_id INT,
    encounter_id INT,
    procedure_description VARCHAR(255),
    load_date DATETIME
);

-- MEDICATIONS
CREATE TABLE cleansed_medications (
    medication_id INT,
    encounter_id INT,
    drug_name VARCHAR(100),
    route VARCHAR(50),
    dose VARCHAR(50),
    frequency VARCHAR(50),
    days_supply INT,
    load_date DATETIME
);