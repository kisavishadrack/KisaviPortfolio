-- =============================================================================
-- Analytical SQL Queries — Health Analytics Pipeline
-- Author : Kisavi Shadrack | shadrackkisavi4@gmail.com
-- Dialect: PostgreSQL 14+
-- =============================================================================


-- ─────────────────────────────────────────────────────────────────────────────
-- QUERY 1: Monthly admissions trend with rolling 3-month average
-- Demonstrates: DATE_TRUNC, window functions, LAG, rolling AVG
-- ─────────────────────────────────────────────────────────────────────────────
WITH monthly AS (
    SELECT
        DATE_TRUNC('month', admission_date)       AS month,
        COUNT(*)                                  AS admissions,
        ROUND(SUM(total_bill_kes)::NUMERIC, 2)   AS revenue_kes
    FROM patient_admissions
    WHERE admission_date IS NOT NULL
    GROUP BY 1
),
with_rolling AS (
    SELECT
        TO_CHAR(month, 'YYYY-MM')                AS month_label,
        admissions,
        revenue_kes,
        ROUND(AVG(admissions) OVER (
            ORDER BY month
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        )::NUMERIC, 1)                            AS rolling_3m_avg_admissions,
        admissions - LAG(admissions) OVER (ORDER BY month)
                                                  AS mom_change,
        ROUND(
            (admissions - LAG(admissions) OVER (ORDER BY month))::NUMERIC
            / NULLIF(LAG(admissions) OVER (ORDER BY month), 0) * 100
        , 1)                                      AS mom_change_pct
    FROM monthly
)
SELECT * FROM with_rolling ORDER BY month_label;


-- ─────────────────────────────────────────────────────────────────────────────
-- QUERY 2: Ward performance scorecard with ranking
-- Demonstrates: CTEs, RANK(), CASE WHEN, conditional aggregation
-- ─────────────────────────────────────────────────────────────────────────────
WITH ward_stats AS (
    SELECT
        ward,
        COUNT(*)                                                AS total_admissions,
        ROUND(AVG(length_of_stay_days)::NUMERIC, 1)            AS avg_los,
        ROUND(AVG(total_bill_kes)::NUMERIC, 2)                 AS avg_bill,
        COUNT(*) FILTER (WHERE discharge_status = 'Recovered') AS recovered,
        COUNT(*) FILTER (WHERE discharge_status = 'Deceased')  AS deaths,
        ROUND(
            COUNT(*) FILTER (WHERE discharge_status = 'Deceased')::NUMERIC
            / NULLIF(COUNT(*), 0) * 100, 2)                   AS mortality_pct,
        ROUND(
            COUNT(*) FILTER (WHERE insurance_covered IN ('Yes','Partial'))::NUMERIC
            / NULLIF(COUNT(*), 0) * 100, 1)                   AS insurance_coverage_pct
    FROM patient_admissions
    WHERE ward IS NOT NULL
    GROUP BY ward
)
SELECT
    ward,
    total_admissions,
    avg_los                                                     AS avg_los_days,
    avg_bill                                                    AS avg_bill_kes,
    mortality_pct,
    insurance_coverage_pct,
    RANK() OVER (ORDER BY total_admissions DESC)               AS admissions_rank,
    RANK() OVER (ORDER BY mortality_pct ASC)                   AS safety_rank,
    CASE
        WHEN mortality_pct > 5  THEN 'HIGH RISK'
        WHEN mortality_pct > 2  THEN 'MODERATE'
        ELSE 'LOW RISK'
    END                                                        AS risk_level
FROM ward_stats
ORDER BY total_admissions DESC;


-- ─────────────────────────────────────────────────────────────────────────────
-- QUERY 3: Top diagnoses by admission volume and average cost
-- Demonstrates: GROUP BY, HAVING, ROUND, ORDER BY multiple columns
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
    diagnosis,
    COUNT(*)                                    AS total_cases,
    ROUND(AVG(total_bill_kes)::NUMERIC, 2)     AS avg_cost_kes,
    ROUND(AVG(length_of_stay_days)::NUMERIC,1) AS avg_los_days,
    ROUND(
        COUNT(*) FILTER (WHERE discharge_status = 'Deceased')::NUMERIC
        / NULLIF(COUNT(*),0) * 100, 2)         AS mortality_rate_pct,
    ROUND(SUM(total_bill_kes)::NUMERIC, 2)     AS total_revenue_kes
FROM patient_admissions
WHERE diagnosis IS NOT NULL
GROUP BY diagnosis
HAVING COUNT(*) >= 5
ORDER BY total_cases DESC, avg_cost_kes DESC
LIMIT 20;


-- ─────────────────────────────────────────────────────────────────────────────
-- QUERY 4: Lab test turnaround time — SLA compliance
-- Demonstrates: CASE, PERCENTILE_CONT, conditional aggregates, CTEs
-- SLA: CBC ≤ 4hrs, Cultures ≤ 72hrs, all others ≤ 24hrs
-- ─────────────────────────────────────────────────────────────────────────────
WITH sla_rules AS (
    SELECT test_name, tat_hours, critical_flag,
        CASE
            WHEN test_name ILIKE '%culture%'       THEN 72
            WHEN test_name ILIKE '%CBC%'
              OR test_name ILIKE '%blood count%'   THEN 4
            ELSE 24
        END AS sla_hours
    FROM lab_results
    WHERE tat_hours IS NOT NULL
)
SELECT
    test_name,
    COUNT(*)                                                AS total_tests,
    ROUND(AVG(tat_hours)::NUMERIC, 1)                      AS avg_tat_hrs,
    ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP
          (ORDER BY tat_hours)::NUMERIC, 1)                 AS median_tat_hrs,
    ROUND(PERCENTILE_CONT(0.9) WITHIN GROUP
          (ORDER BY tat_hours)::NUMERIC, 1)                 AS p90_tat_hrs,
    sla_hours,
    COUNT(*) FILTER (WHERE tat_hours <= sla_hours)         AS within_sla,
    ROUND(
        COUNT(*) FILTER (WHERE tat_hours <= sla_hours)::NUMERIC
        / NULLIF(COUNT(*), 0) * 100, 1)                    AS sla_compliance_pct,
    COUNT(*) FILTER (WHERE critical_flag = TRUE)           AS critical_count
FROM sla_rules
GROUP BY test_name, sla_hours
HAVING COUNT(*) >= 5
ORDER BY sla_compliance_pct ASC;


-- ─────────────────────────────────────────────────────────────────────────────
-- QUERY 5: OPD patient flow — busiest clinics & wait time analysis
-- Demonstrates: Multiple aggregations, FILTER, ROUND, ranking
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
    clinic,
    COUNT(*)                                                AS total_visits,
    ROUND(AVG(wait_time_minutes)::NUMERIC, 1)              AS avg_wait_min,
    MIN(wait_time_minutes)                                  AS min_wait_min,
    MAX(wait_time_minutes)                                  AS max_wait_min,
    ROUND(PERCENTILE_CONT(0.9) WITHIN GROUP
          (ORDER BY wait_time_minutes)::NUMERIC, 0)         AS p90_wait_min,
    ROUND(AVG(consult_duration_min)::NUMERIC, 1)           AS avg_consult_min,
    ROUND(SUM(consultation_fee_kes)::NUMERIC, 2)           AS total_fees_kes,
    COUNT(*) FILTER (WHERE follow_up_required = TRUE)      AS follow_ups,
    ROUND(
        COUNT(*) FILTER (WHERE follow_up_required = TRUE)::NUMERIC
        / NULLIF(COUNT(*), 0) * 100, 1)                    AS follow_up_pct,
    MODE() WITHIN GROUP (ORDER BY payment_method)          AS top_payment
FROM outpatient_visits
WHERE clinic IS NOT NULL
GROUP BY clinic
ORDER BY total_visits DESC;


-- ─────────────────────────────────────────────────────────────────────────────
-- QUERY 6: 360° Patient Journey — admissions + OPD + lab tests per patient
-- Demonstrates: Multi-table JOINs, subqueries, COALESCE, window functions
-- ─────────────────────────────────────────────────────────────────────────────
WITH patient_summary AS (
    SELECT
        p.patient_id,
        p.gender,
        p.county,
        COUNT(DISTINCT a.admission_id)     AS total_admissions,
        COUNT(DISTINCT o.visit_id)         AS total_opd_visits,
        COUNT(DISTINCT l.lab_result_id)    AS total_lab_tests,
        COALESCE(SUM(a.total_bill_kes), 0)
          + COALESCE(SUM(o.consultation_fee_kes), 0)  AS total_spend_kes,
        MAX(a.admission_date)              AS last_admission,
        MAX(o.visit_date)                  AS last_opd_visit,
        COUNT(DISTINCT l.lab_result_id)
            FILTER (WHERE l.critical_flag = TRUE)     AS critical_labs
    FROM patients p
    LEFT JOIN patient_admissions  a ON p.patient_id = a.patient_id
    LEFT JOIN outpatient_visits   o ON p.patient_id = o.patient_id
    LEFT JOIN lab_results         l ON p.patient_id = l.patient_id
    GROUP BY p.patient_id, p.gender, p.county
)
SELECT
    patient_id,
    gender,
    county,
    total_admissions,
    total_opd_visits,
    total_lab_tests,
    ROUND(total_spend_kes::NUMERIC, 2)     AS total_spend_kes,
    critical_labs,
    NTILE(4) OVER (ORDER BY total_spend_kes DESC) AS spend_quartile,
    CASE
        WHEN total_admissions >= 3 THEN 'Frequent Inpatient'
        WHEN total_opd_visits  >= 5 THEN 'Frequent OPD'
        WHEN critical_labs     >= 1 THEN 'High Clinical Risk'
        ELSE 'Standard'
    END AS patient_segment
FROM patient_summary
WHERE (total_admissions + total_opd_visits + total_lab_tests) > 0
ORDER BY total_spend_kes DESC
LIMIT 50;


-- ─────────────────────────────────────────────────────────────────────────────
-- QUERY 7: County-level health burden summary (for geo dashboard)
-- Demonstrates: GROUP BY, ROUND, multiple metrics in one pass
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
    p.county,
    COUNT(DISTINCT p.patient_id)                           AS unique_patients,
    COUNT(DISTINCT a.admission_id)                         AS total_admissions,
    COUNT(DISTINCT o.visit_id)                             AS total_opd_visits,
    COUNT(DISTINCT l.lab_result_id)                        AS total_lab_tests,
    ROUND(AVG(a.length_of_stay_days)::NUMERIC, 1)         AS avg_los_days,
    ROUND(SUM(a.total_bill_kes)::NUMERIC, 2)              AS inpatient_revenue_kes,
    ROUND(SUM(o.consultation_fee_kes)::NUMERIC, 2)        AS opd_revenue_kes,
    ROUND(
        COUNT(DISTINCT a.admission_id)::NUMERIC
        / NULLIF(COUNT(DISTINCT p.patient_id), 0), 2)     AS admissions_per_patient
FROM patients p
LEFT JOIN patient_admissions a ON p.patient_id = a.patient_id
LEFT JOIN outpatient_visits  o ON p.patient_id = o.patient_id
LEFT JOIN lab_results        l ON p.patient_id = l.patient_id
WHERE p.county IS NOT NULL
GROUP BY p.county
ORDER BY total_admissions DESC;


-- ─────────────────────────────────────────────────────────────────────────────
-- QUERY 8: Doctor workload & outcome analysis
-- Demonstrates: Complex filtering, CASE bucketing, performance metrics
-- ─────────────────────────────────────────────────────────────────────────────
WITH doctor_adm AS (
    SELECT
        attending_doctor,
        COUNT(*)                                            AS admissions,
        ROUND(AVG(length_of_stay_days)::NUMERIC, 1)       AS avg_los,
        ROUND(AVG(total_bill_kes)::NUMERIC, 2)            AS avg_bill,
        COUNT(*) FILTER (WHERE discharge_status = 'Recovered') AS recoveries,
        COUNT(*) FILTER (WHERE discharge_status = 'Deceased')  AS deaths,
        ROUND(
            COUNT(*) FILTER (WHERE discharge_status = 'Deceased')::NUMERIC
            / NULLIF(COUNT(*), 0) * 100, 2)               AS mortality_pct
    FROM patient_admissions
    WHERE attending_doctor IS NOT NULL
      AND attending_doctor NOT IN ('Unknown')
    GROUP BY attending_doctor
),
doctor_opd AS (
    SELECT
        attending_doctor,
        COUNT(*)                                           AS opd_visits,
        ROUND(AVG(wait_time_minutes)::NUMERIC, 1)         AS avg_patient_wait
    FROM outpatient_visits
    WHERE attending_doctor IS NOT NULL
      AND attending_doctor NOT IN ('Unknown')
    GROUP BY attending_doctor
)
SELECT
    a.attending_doctor,
    a.admissions,
    COALESCE(o.opd_visits, 0)                             AS opd_visits,
    a.admissions + COALESCE(o.opd_visits, 0)             AS total_caseload,
    a.avg_los                                             AS avg_inpatient_los,
    a.avg_bill                                            AS avg_bill_kes,
    a.mortality_pct,
    COALESCE(o.avg_patient_wait, 0)                       AS avg_opd_wait_min,
    RANK() OVER (ORDER BY a.admissions + COALESCE(o.opd_visits,0) DESC)
                                                          AS workload_rank
FROM doctor_adm a
LEFT JOIN doctor_opd o USING (attending_doctor)
ORDER BY total_caseload DESC;
