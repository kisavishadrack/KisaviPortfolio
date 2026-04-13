# Power BI Connection Guide
## Hospital Health Analytics — Kisavi Shadrack

---

## Option A: Connect Power BI to PostgreSQL (Live Connection)

### Step 1: Get Data
- Open Power BI Desktop → **Get Data** → **PostgreSQL database**
- Server: `localhost` (or your server IP)
- Database: `health_analytics_db`
- Mode: **Import** (for portfolio) or **DirectQuery** (for live)

### Step 2: Select Views
Select these tables/views from `health_analytics` schema:
- `v_patient_summary`
- `v_ward_kpis`
- `v_lab_performance`
- `v_monthly_activity`
- `v_high_risk_patients`

### Step 3: Relationships
Power BI will auto-detect, but verify:
- `stg_patients[patient_id]` → `stg_admissions[patient_id]` (1:Many)
- `stg_patients[patient_id]` → `stg_lab_tests[patient_id]` (1:Many)
- `stg_patients[patient_id]` → `stg_outpatient_visits[patient_id]` (1:Many)

---

## Option B: Connect to Clean CSV Files (No PostgreSQL needed)

1. **Get Data** → **Text/CSV** → import `output/stg_patients.csv`
2. Repeat for the other 3 clean CSVs
3. Set up relationships manually in Model view

---

## Recommended Visuals to Build

### Page 1 — Overview
- Card visuals: Total Patients, Admissions, Lab Tests, OPD Visits
- Line chart: Monthly admissions trend
- Donut: Revenue split (Inpatient vs Outpatient)

### Page 2 — Ward Analytics
- Clustered bar: Admissions by ward
- KPI card: Avg Length of Stay
- Table with conditional formatting: Ward mortality rates

### Page 3 — Lab Performance
- Bar chart: Abnormal rate by test (red threshold line at 40%)
- Scatter: TAT vs abnormal rate
- Gauge: Overall abnormal rate (43.8%)

### Page 4 — OPD Clinics
- Bar: Visit volume by clinic
- Bar: Avg wait time by clinic (color-coded by threshold)
- Slicer: Filter by clinic

### Page 5 — Patient Demographics
- Map visual: Patients by county (Kenya map)
- Donut: Gender split
- Table: High-risk patient list

---

## Useful DAX Measures

```dax
Mortality Rate % =
DIVIDE(
    COUNTROWS(FILTER(stg_admissions, stg_admissions[discharge_outcome] = "Deceased")),
    COUNTROWS(stg_admissions),
    0
) * 100

Avg LOS =
AVERAGE(stg_admissions[length_of_stay])

Abnormal Lab % =
DIVIDE(
    COUNTROWS(FILTER(stg_lab_tests, stg_lab_tests[is_abnormal] = TRUE())),
    COUNTROWS(stg_lab_tests),
    0
) * 100

Total Revenue =
SUM(stg_admissions[total_cost_kes]) +
SUM(stg_outpatient_visits[consultation_fee_kes])
```
