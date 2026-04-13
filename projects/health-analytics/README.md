# 🏥 Hospital Health Analytics Pipeline
### Portfolio Project — Kisavi Shadrack | Data Analyst

[![Python](https://img.shields.io/badge/Python-3.10%2B-blue)](https://python.org)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-14%2B-336791)](https://postgresql.org)
[![SQLite](https://img.shields.io/badge/SQLite-Local%20Demo-green)](https://sqlite.org)
[![Chart.js](https://img.shields.io/badge/Dashboard-Chart.js-ff6384)](https://chartjs.org)

---

## 📌 Overview

An end-to-end **healthcare data engineering and analytics project** that:

1. Generates realistic, messy HMIS/LIMS Excel exports (admissions, lab, OPD)
2. Cleans and validates the data through a Python ETL pipeline
3. Loads into **PostgreSQL** (production) or **SQLite** (local demo)
4. Runs **8 advanced analytical SQL queries** with CTEs, window functions, stored procedures, and materialised views
5. Delivers an **interactive clinical dashboard** (HTML/JS) and a **Power BI–ready data model**

This project directly mirrors real work done at Dr. Kalebi Labs (DKL) Ltd — LIMS integration, patient data pipelines, and clinical dashboards.

---

## 🗂️ Project Structure

```
health_analytics_pipeline/
├── python/
│   ├── generate_health_data.py    # Messy dataset generator (3 Excel files)
│   └── etl_pipeline.py            # Full ETL: Extract → Transform → Load → Report
├── sql/
│   ├── schema/
│   │   └── 01_create_tables.sql   # PostgreSQL DDL: 5 tables, indexes, constraints
│   ├── stored_procedures/
│   │   └── 02_procedures_views.sql # 4 materialised views, 2 stored procedures
│   └── queries/
│       └── 03_analytical_queries.sql # 8 advanced analytical queries
├── dashboard/
│   └── dashboard.html             # Interactive 4-tab clinical dashboard
├── data/
│   ├── patient_admissions.xlsx    # 420 rows, 13 columns (messy)
│   ├── lab_results.xlsx           # 525 rows, 13 columns (messy)
│   ├── outpatient_visits.xlsx     # 630 rows, 15 columns (messy)
│   └── health_analytics.db        # SQLite database (post-ETL)
├── reports/
│   └── etl_summary_report.xlsx   # ETL run summary + query results
└── logs/
    └── etl_pipeline.log           # Timestamped run log
```

---

## 🔬 Data Quality Issues Handled

| Dataset | Issue Types |
|---|---|
| **Patient Admissions** | Duplicate rows, inconsistent ward/diagnosis casing, discharge before admission, negative bills, impossible ages, mixed date formats |
| **Lab Results** | Test name synonyms (CBC/Complete Blood Count), result date before order date, negative TAT, non-numeric results in numeric fields |
| **Outpatient Visits** | Payment method variants (mpesa/Mpesa/MPESA), impossible wait times, negative fees, malformed Visit IDs |

---

## 🗄️ SQL Highlights

| Query | Techniques Used |
|---|---|
| Monthly admissions trend | `DATE_TRUNC`, `LAG()`, rolling `AVG() OVER` |
| Ward performance scorecard | CTE, `RANK()`, `CASE WHEN`, conditional aggregation |
| Top diagnoses by cost | `GROUP BY`, `HAVING`, multi-column `ORDER BY` |
| Lab SLA compliance | CTE, `PERCENTILE_CONT`, `FILTER`, business rules logic |
| OPD wait time analysis | `MODE()`, `PERCENTILE_CONT`, multi-metric aggregation |
| 360° Patient Journey | Multi-table `LEFT JOIN`, `COALESCE`, `NTILE()`, segmentation |
| County health burden | 4-table join, per-patient ratios |
| Doctor workload | Multi-CTE, `RANK()`, cross-dataset join |

---

## 📊 Dashboard Features

4-tab interactive dashboard (`dashboard/dashboard.html`) — open directly in browser, no server needed:

- **Overview**: KPI strip, monthly trend, discharge outcomes, top diagnoses
- **Admissions**: Revenue by ward, mortality heatmap, full ward scorecard table
- **Lab Analytics**: Test volume, TAT compliance, critical flags
- **Outpatient**: Clinic load, payment method mix, wait time analysis

---

## 🚀 How to Run

### Local demo (SQLite — no PostgreSQL needed)
```bash
# 1. Generate messy data
python python/generate_health_data.py

# 2. Run ETL pipeline
python python/etl_pipeline.py --mode sqlite

# 3. Open dashboard
open dashboard/dashboard.html
```

### PostgreSQL (production mode)
```bash
# 1. Set up schema
psql -U postgres -d health_db -f sql/schema/01_create_tables.sql
psql -U postgres -d health_db -f sql/stored_procedures/02_procedures_views.sql

# 2. Run ETL
python python/etl_pipeline.py --mode postgres --dsn "postgresql://user:pw@localhost/health_db"

# 3. Run analytical queries
psql -U postgres -d health_db -f sql/queries/03_analytical_queries.sql
```

---

## 📈 ETL Results

```
patient_admissions.xlsx : 420 raw → 339 clean (20 dupes, 61 rejected)
lab_results.xlsx        : 525 raw → 422 clean (25 dupes, 78 rejected)
outpatient_visits.xlsx  : 630 raw → 503 clean (30 dupes, 97 rejected)
Run Time                : < 2 seconds
```

---

## 🛠️ Tech Stack

- **Python** — ETL pipeline (pandas, openpyxl, sqlite3, argparse, logging)
- **PostgreSQL 14+** — Production database with constraints, indexes, generated columns
- **SQLite** — Local demo mode, zero-install
- **SQL** — CTEs, window functions, stored procedures, materialised views
- **Chart.js** — Interactive browser dashboard
- **HTML/CSS/JS** — No framework dependency, runs offline

---

## 👤 Author

**Kisavi Shadrack** — Data Analyst  
📧 shadrackkisavi4@gmail.com  
📍 Nairobi, Kenya  
🔗 [Portfolio](https://kisavishadrack.github.io/Shadrackanalyst.github.io/)

*Inspired by real-world work at Dr. Kalebi Labs (DKL) Ltd and BITLOGIX LIMITED*
