-- ============================================================
-- SCHEMA: health_analytics
-- Project: Hospital Health Analytics Pipeline
-- Author:  Kisavi Shadrack | shadrackkisavi4@gmail.com
-- ============================================================

CREATE SCHEMA IF NOT EXISTS health_analytics;
SET search_path TO health_analytics;

-- RAW TABLES
CREATE TABLE IF NOT EXISTS raw_patients (
    patient_id TEXT, first_name TEXT, last_name TEXT,
    date_of_birth TEXT, gender TEXT, blood_group TEXT,
    county TEXT, phone TEXT, registration_date TEXT,
    is_active TEXT, loaded_at TIMESTAMP DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS raw_admissions (
    admission_id TEXT, patient_id TEXT, admission_date TEXT,
    discharge_date TEXT, ward TEXT, diagnosis TEXT,
    attending_doctor TEXT, total_cost_kes TEXT,
    discharge_outcome TEXT, bed_number TEXT,
    loaded_at TIMESTAMP DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS raw_lab_tests (
    lab_id TEXT, patient_id TEXT, admission_id TEXT,
    test_name TEXT, test_code TEXT, ordered_date TEXT,
    result_date TEXT, result_value TEXT, unit TEXT,
    reference_range TEXT, result_status TEXT,
    technician TEXT, loaded_at TIMESTAMP DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS raw_outpatient_visits (
    visit_id TEXT, patient_id TEXT, visit_date TEXT,
    clinic TEXT, chief_complaint TEXT, attending_doctor TEXT,
    wait_time_mins TEXT, consultation_fee_kes TEXT,
    outcome TEXT, follow_up_date TEXT,
    loaded_at TIMESTAMP DEFAULT NOW()
);

-- CLEAN STAGING TABLES
CREATE TABLE IF NOT EXISTS stg_patients (
    patient_id VARCHAR(20) PRIMARY KEY,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    date_of_birth DATE, age_years INTEGER,
    gender VARCHAR(10), blood_group VARCHAR(5),
    county VARCHAR(50), phone VARCHAR(20),
    registration_date DATE, is_active BOOLEAN DEFAULT TRUE,
    cleaned_at TIMESTAMP DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS stg_admissions (
    admission_id VARCHAR(20) PRIMARY KEY,
    patient_id VARCHAR(20),
    admission_date DATE NOT NULL,
    discharge_date DATE,
    length_of_stay INTEGER,
    ward VARCHAR(50), diagnosis VARCHAR(200),
    attending_doctor VARCHAR(100),
    total_cost_kes NUMERIC(12,2),
    discharge_outcome VARCHAR(50),
    bed_number INTEGER,
    cleaned_at TIMESTAMP DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS stg_lab_tests (
    lab_id VARCHAR(20) PRIMARY KEY,
    patient_id VARCHAR(20), admission_id VARCHAR(20),
    test_name VARCHAR(100) NOT NULL, test_code VARCHAR(10),
    ordered_date DATE NOT NULL, result_date DATE,
    turnaround_hours NUMERIC(8,2),
    result_value NUMERIC(12,4), unit VARCHAR(30),
    reference_range VARCHAR(50), result_status VARCHAR(20),
    is_abnormal BOOLEAN, technician VARCHAR(20),
    cleaned_at TIMESTAMP DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS stg_outpatient_visits (
    visit_id VARCHAR(20) PRIMARY KEY,
    patient_id VARCHAR(20),
    visit_date DATE NOT NULL,
    clinic VARCHAR(80), chief_complaint VARCHAR(100),
    attending_doctor VARCHAR(100),
    wait_time_mins INTEGER,
    consultation_fee_kes NUMERIC(10,2),
    outcome VARCHAR(80), follow_up_date DATE,
    cleaned_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_adm_patient ON stg_admissions(patient_id);
CREATE INDEX IF NOT EXISTS idx_adm_date ON stg_admissions(admission_date);
CREATE INDEX IF NOT EXISTS idx_lab_patient ON stg_lab_tests(patient_id);
CREATE INDEX IF NOT EXISTS idx_opd_patient ON stg_outpatient_visits(patient_id);
CREATE INDEX IF NOT EXISTS idx_opd_date ON stg_outpatient_visits(visit_date);
