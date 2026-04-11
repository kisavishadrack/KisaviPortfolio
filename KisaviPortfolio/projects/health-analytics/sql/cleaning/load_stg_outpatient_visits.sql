-- Auto-generated INSERT statements for stg_stg_outpatient_visits
-- Generated: 2026-04-01 07:32:56.047994

TRUNCATE TABLE health_analytics.stg_stg_outpatient_visits CASCADE;

INSERT INTO health_analytics.stg_stg_outpatient_visits (visit_id, patient_id, visit_date, clinic, chief_complaint, attending_doctor, wait_time_mins, consultation_fee_kes, outcome, follow_up_date) VALUES ('OPD-000093', 'PAT-0360', '2022-02-01', 'Dental', 'Cough', 'Dr. Otieno', 237, 2633.16, 'Investigations ordered', '2022-03-02');
INSERT INTO health_analytics.stg_stg_outpatient_visits (visit_id, patient_id, visit_date, clinic, chief_complaint, attending_doctor, wait_time_mins, consultation_fee_kes, outcome, follow_up_date) VALUES ('OPD-000641', 'PAT-0221', '2023-11-17', 'Paediatric Opd', 'Cough', 'Dr. Kariuki', 52, 779.77, 'Investigations ordered', NULL);
-- ... 791 total INSERT rows generated
