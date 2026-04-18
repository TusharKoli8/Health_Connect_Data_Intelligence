USE dev_HealthConnect_refined;

-- PATIENTS
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
CREATE TABLE refined_providers (
    provider_id INT PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    specialty VARCHAR(100),
    npi VARCHAR(50),
    load_date DATETIME
);

-- PAYERS
CREATE TABLE refined_payers (
    payer_id INT PRIMARY KEY,
    payer_name VARCHAR(100),
    load_date DATETIME
);

-- ENCOUNTERS
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

CREATE TABLE refined_diagnoses (
    diagnosis_id INT PRIMARY KEY,
    patient_id INT,
    encounter_id INT,
    diagnosis_code VARCHAR(50),
    diagnosis_description VARCHAR(255),
    load_date DATETIME
);


CREATE TABLE refined_procedures (
    procedure_id INT PRIMARY KEY,
    patient_id INT,
    encounter_id INT,
    procedure_code VARCHAR(50),
    procedure_description VARCHAR(255),
    load_date DATETIME
);

CREATE TABLE refined_medications (
    medication_id INT PRIMARY KEY,
    patient_id INT,
    encounter_id INT,
    medication_name VARCHAR(100),
    dosage VARCHAR(50),
    load_date DATETIME
);