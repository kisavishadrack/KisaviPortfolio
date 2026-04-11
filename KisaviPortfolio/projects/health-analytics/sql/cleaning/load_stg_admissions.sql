-- Auto-generated INSERT statements for stg_stg_admissions
-- Generated: 2026-04-01 07:32:55.985251

TRUNCATE TABLE health_analytics.stg_stg_admissions CASCADE;

INSERT INTO health_analytics.stg_stg_admissions (admission_id, patient_id, admission_date, discharge_date, ward, diagnosis, attending_doctor, total_cost_kes, discharge_outcome, bed_number, length_of_stay) VALUES ('ADM-00308', 'PAT-0148', '2023-04-26', '2023-05-02', 'Surgical', 'Acute Gastroenteritis', 'Dr. Kariuki', 17564.05, 'Deceased', 27, 6.0);
INSERT INTO health_analytics.stg_stg_admissions (admission_id, patient_id, admission_date, discharge_date, ward, diagnosis, attending_doctor, total_cost_kes, discharge_outcome, bed_number, length_of_stay) VALUES ('ADM-00532', 'PAT-0182', '2023-07-17', '2023-08-03', 'Oncology', 'Hiv/Aids', 'Dr. Chebet', NULL, 'Absconded', 95, 17.0);
-- ... 519 total INSERT rows generated
