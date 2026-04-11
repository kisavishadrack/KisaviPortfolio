-- Auto-generated INSERT statements for stg_stg_lab_tests
-- Generated: 2026-04-01 07:32:56.007589

TRUNCATE TABLE health_analytics.stg_stg_lab_tests CASCADE;

INSERT INTO health_analytics.stg_stg_lab_tests (lab_id, patient_id, admission_id, test_name, test_code, ordered_date, result_date, result_value, unit, reference_range, result_status, technician, turnaround_hours, is_abnormal) VALUES ('LAB-000457', 'PAT-0205', 'ADM-00140', 'Creatinine', 'CRE', '2023-02-20', '2023-02-20', 165.88, 'µmol/L', NULL, 'Abnormal', 'Tech-2', NULL, True);
INSERT INTO health_analytics.stg_stg_lab_tests (lab_id, patient_id, admission_id, test_name, test_code, ordered_date, result_date, result_value, unit, reference_range, result_status, technician, turnaround_hours, is_abnormal) VALUES ('LAB-000851', 'PAT-0097', NULL, 'Cd4 Count', 'CD4', '2022-04-13', '2022-04-14', 1867.15, 'cells/µL', '200 - 1500', 'Critical', 'Tech-7', 24.0, True);
-- ... 1117 total INSERT rows generated
