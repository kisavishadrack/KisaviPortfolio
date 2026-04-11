-- =============================================================================
-- Stored Procedures & Views — Health Analytics Pipeline
-- Author : Kisavi Shadrack
-- =============================================================================

-- ─────────────────────────────────────────────────────────────────────────────
-- STORED PROCEDURE: Refresh all analytical summary tables
-- ─────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE PROCEDURE refresh_analytics()
LANGUAGE plpgsql AS $$
BEGIN
    -- Re-create all materialised views
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_monthly_admissions;
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_ward_performance;
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_lab_turnaround;
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_opd_clinic_load;
    RAISE NOTICE 'Analytics views refreshed at %', NOW();
END;
$$;

-- ─────────────────────────────────────────────────────────────────────────────
-- STORED PROCEDURE: Validate data quality of a newly loaded batch
-- Returns a table of issues found
-- ─────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION validate_batch(p_source VARCHAR)
RETURNS TABLE(check_name TEXT, issue_count BIGINT, severity TEXT)
LANGUAGE plpgsql AS $$
BEGIN
    -- Admissions: discharge before admission
    RETURN QUERY
    SELECT  'Discharge before Admission'::TEXT,
            COUNT(*),
            'CRITICAL'::TEXT
    FROM    patient_admissions
    WHERE   discharge_date < admission_date
      AND   data_source = p_source;

    -- Admissions: missing diagnosis
    RETURN QUERY
    SELECT  'Missing Diagnosis'::TEXT,
            COUNT(*),
            'WARNING'::TEXT
    FROM    patient_admissions
    WHERE   diagnosis IS NULL
      AND   data_source = p_source;

    -- Lab: result date before order date
    RETURN QUERY
    SELECT  'Lab Result Before Order Date'::TEXT,
            COUNT(*),
            'CRITICAL'::TEXT
    FROM    lab_results
    WHERE   date_resulted < date_ordered
      AND   data_source = p_source;

    -- Lab: TAT > 7 days (168 hours) — possible data entry error
    RETURN QUERY
    SELECT  'Lab TAT > 7 Days'::TEXT,
            COUNT(*),
            'WARNING'::TEXT
    FROM    lab_results
    WHERE   tat_hours > 168
      AND   data_source = p_source;

    -- OPD: wait time > 8 hours — likely data error
    RETURN QUERY
    SELECT  'OPD Wait Time > 8 Hours'::TEXT,
            COUNT(*),
            'WARNING'::TEXT
    FROM    outpatient_visits
    WHERE   wait_time_minutes > 480
      AND   data_source = p_source;

    -- OPD: negative consultation fee
    RETURN QUERY
    SELECT  'Negative Consultation Fee'::TEXT,
            COUNT(*),
            'CRITICAL'::TEXT
    FROM    outpatient_visits
    WHERE   consultation_fee_kes < 0
      AND   data_source = p_source;
END;
$$;

-- ─────────────────────────────────────────────────────────────────────────────
-- MATERIALISED VIEW: Monthly admissions & revenue trend
-- ─────────────────────────────────────────────────────────────────────────────
CREATE MATERIALIZED VIEW mv_monthly_admissions AS
SELECT
    DATE_TRUNC('month', admission_date)         AS month,
    TO_CHAR(admission_date, 'YYYY-MM')          AS month_label,
    COUNT(*)                                    AS total_admissions,
    COUNT(*) FILTER (WHERE discharge_status = 'Deceased')
                                                AS deaths,
    ROUND(
        COUNT(*) FILTER (WHERE discharge_status = 'Deceased')::NUMERIC
        / NULLIF(COUNT(*), 0) * 100, 2)         AS mortality_rate_pct,
    ROUND(AVG(length_of_stay_days)::NUMERIC, 1) AS avg_los_days,
    ROUND(SUM(total_bill_kes)::NUMERIC, 2)      AS total_revenue_kes,
    ROUND(AVG(total_bill_kes)::NUMERIC, 2)      AS avg_bill_kes
FROM patient_admissions
WHERE admission_date IS NOT NULL
GROUP BY 1, 2
ORDER BY 1;

CREATE UNIQUE INDEX ON mv_monthly_admissions(month);

-- ─────────────────────────────────────────────────────────────────────────────
-- MATERIALISED VIEW: Ward-level performance
-- ─────────────────────────────────────────────────────────────────────────────
CREATE MATERIALIZED VIEW mv_ward_performance AS
SELECT
    ward,
    COUNT(*)                                        AS total_admissions,
    ROUND(AVG(length_of_stay_days)::NUMERIC, 1)    AS avg_los_days,
    ROUND(AVG(total_bill_kes)::NUMERIC, 2)          AS avg_bill_kes,
    ROUND(SUM(total_bill_kes)::NUMERIC, 2)          AS total_revenue_kes,
    COUNT(*) FILTER (WHERE discharge_status = 'Recovered') AS recovered,
    COUNT(*) FILTER (WHERE discharge_status = 'Deceased')  AS deaths,
    COUNT(*) FILTER (WHERE discharge_status = 'Referred')  AS referred,
    ROUND(
        COUNT(*) FILTER (WHERE insurance_covered IN ('Yes','Partial'))::NUMERIC
        / NULLIF(COUNT(*), 0) * 100, 1)             AS insurance_pct
FROM patient_admissions
WHERE ward IS NOT NULL
GROUP BY ward
ORDER BY total_admissions DESC;

CREATE UNIQUE INDEX ON mv_ward_performance(ward);

-- ─────────────────────────────────────────────────────────────────────────────
-- MATERIALISED VIEW: Lab test turnaround time analysis
-- ─────────────────────────────────────────────────────────────────────────────
CREATE MATERIALIZED VIEW mv_lab_turnaround AS
SELECT
    test_name,
    COUNT(*)                                    AS total_tests,
    COUNT(*) FILTER (WHERE result_status = 'Final')   AS final_results,
    COUNT(*) FILTER (WHERE critical_flag = TRUE)      AS critical_results,
    ROUND(AVG(tat_hours)::NUMERIC, 1)           AS avg_tat_hours,
    MIN(tat_hours)                              AS min_tat_hours,
    MAX(tat_hours)                              AS max_tat_hours,
    ROUND(
        PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY tat_hours)::NUMERIC
    , 1)                                        AS p90_tat_hours
FROM lab_results
WHERE tat_hours IS NOT NULL
  AND test_name IS NOT NULL
GROUP BY test_name
ORDER BY total_tests DESC;

CREATE UNIQUE INDEX ON mv_lab_turnaround(test_name);

-- ─────────────────────────────────────────────────────────────────────────────
-- MATERIALISED VIEW: OPD clinic load & wait times
-- ─────────────────────────────────────────────────────────────────────────────
CREATE MATERIALIZED VIEW mv_opd_clinic_load AS
SELECT
    clinic,
    COUNT(*)                                    AS total_visits,
    ROUND(AVG(wait_time_minutes)::NUMERIC, 1)   AS avg_wait_min,
    ROUND(AVG(consult_duration_min)::NUMERIC,1) AS avg_consult_min,
    ROUND(SUM(consultation_fee_kes)::NUMERIC,2) AS total_fees_kes,
    COUNT(*) FILTER (WHERE follow_up_required = TRUE) AS follow_ups,
    ROUND(
        COUNT(*) FILTER (WHERE follow_up_required = TRUE)::NUMERIC
        / NULLIF(COUNT(*), 0) * 100, 1)         AS follow_up_pct,
    MODE() WITHIN GROUP (ORDER BY payment_method) AS top_payment_method
FROM outpatient_visits
WHERE clinic IS NOT NULL
GROUP BY clinic
ORDER BY total_visits DESC;

CREATE UNIQUE INDEX ON mv_opd_clinic_load(clinic);
