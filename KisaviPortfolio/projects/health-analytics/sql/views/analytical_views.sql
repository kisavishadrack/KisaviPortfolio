-- ============================================================
-- ANALYTICAL VIEWS — health_analytics schema
-- Author: Kisavi Shadrack
-- ============================================================
SET search_path TO health_analytics;

-- View 1: Patient summary with admission & visit counts
CREATE OR REPLACE VIEW v_patient_summary AS
SELECT
    p.patient_id,
    p.first_name || ' ' || p.last_name  AS full_name,
    p.age_years,
    p.gender,
    p.county,
    p.blood_group,
    COUNT(DISTINCT a.admission_id)       AS total_admissions,
    COUNT(DISTINCT o.visit_id)           AS total_opd_visits,
    COUNT(DISTINCT l.lab_id)             AS total_lab_tests,
    ROUND(SUM(a.total_cost_kes)::NUMERIC, 0) AS total_inpatient_spend,
    MAX(a.admission_date)                AS last_admission
FROM stg_patients p
LEFT JOIN stg_admissions a       ON p.patient_id = a.patient_id
LEFT JOIN stg_outpatient_visits o ON p.patient_id = o.patient_id
LEFT JOIN stg_lab_tests l        ON p.patient_id = l.patient_id
GROUP BY p.patient_id, p.first_name, p.last_name,
         p.age_years, p.gender, p.county, p.blood_group;

-- View 2: Ward performance KPIs
CREATE OR REPLACE VIEW v_ward_kpis AS
SELECT
    ward,
    COUNT(*)                                          AS total_admissions,
    ROUND(AVG(length_of_stay), 1)                    AS avg_length_of_stay,
    ROUND(AVG(total_cost_kes), 0)                    AS avg_cost_kes,
    SUM(CASE WHEN discharge_outcome = 'Deceased' THEN 1 ELSE 0 END) AS mortality_count,
    ROUND(100.0 * SUM(CASE WHEN discharge_outcome = 'Deceased' THEN 1 ELSE 0 END)
          / NULLIF(COUNT(*), 0), 2)                  AS mortality_rate_pct,
    ROUND(SUM(total_cost_kes), 0)                    AS total_revenue
FROM stg_admissions
WHERE ward IS NOT NULL
GROUP BY ward;

-- View 3: Lab test performance
CREATE OR REPLACE VIEW v_lab_performance AS
SELECT
    test_name,
    test_code,
    COUNT(*)                                              AS total_ordered,
    SUM(CASE WHEN result_date IS NOT NULL THEN 1 ELSE 0 END) AS results_returned,
    ROUND(100.0 * SUM(CASE WHEN result_date IS NOT NULL THEN 1 ELSE 0 END)
          / NULLIF(COUNT(*), 0), 1)                       AS completion_rate_pct,
    ROUND(AVG(turnaround_hours), 1)                       AS avg_tat_hours,
    ROUND(100.0 * SUM(CASE WHEN is_abnormal THEN 1 ELSE 0 END)
          / NULLIF(COUNT(*), 0), 1)                       AS abnormal_rate_pct
FROM stg_lab_tests
GROUP BY test_name, test_code;

-- View 4: Monthly activity trend
CREATE OR REPLACE VIEW v_monthly_activity AS
SELECT
    DATE_TRUNC('month', activity_date)  AS month,
    activity_type,
    COUNT(*)                              AS volume,
    ROUND(SUM(revenue), 0)               AS revenue
FROM (
    SELECT admission_date AS activity_date, 'Admission' AS activity_type, total_cost_kes AS revenue
    FROM stg_admissions WHERE admission_date IS NOT NULL
    UNION ALL
    SELECT visit_date, 'OPD Visit', consultation_fee_kes
    FROM stg_outpatient_visits WHERE visit_date IS NOT NULL
    UNION ALL
    SELECT ordered_date, 'Lab Test', NULL
    FROM stg_lab_tests WHERE ordered_date IS NOT NULL
) combined
GROUP BY DATE_TRUNC('month', activity_date), activity_type
ORDER BY month, activity_type;

-- View 5: High-risk patients (multiple admissions + abnormal labs)
CREATE OR REPLACE VIEW v_high_risk_patients AS
WITH adm_counts AS (
    SELECT patient_id, COUNT(*) AS admissions,
           ROUND(AVG(length_of_stay), 1) AS avg_los
    FROM stg_admissions GROUP BY patient_id
),
lab_abnormal AS (
    SELECT patient_id,
           SUM(CASE WHEN is_abnormal THEN 1 ELSE 0 END) AS abnormal_results,
           COUNT(*) AS total_labs
    FROM stg_lab_tests GROUP BY patient_id
)
SELECT p.patient_id, p.first_name || ' ' || p.last_name AS full_name,
       p.age_years, p.gender, p.county,
       COALESCE(a.admissions, 0)        AS admissions,
       COALESCE(a.avg_los, 0)           AS avg_los,
       COALESCE(l.abnormal_results, 0)  AS abnormal_lab_results,
       COALESCE(l.total_labs, 0)        AS total_labs
FROM stg_patients p
LEFT JOIN adm_counts a  ON p.patient_id = a.patient_id
LEFT JOIN lab_abnormal l ON p.patient_id = l.patient_id
WHERE COALESCE(a.admissions, 0) > 1
   OR COALESCE(l.abnormal_results, 0) > 2
ORDER BY admissions DESC, abnormal_lab_results DESC;
