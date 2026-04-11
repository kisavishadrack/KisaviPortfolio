"""
generate_health_data.py
=======================
Generates four messy, interlinked health datasets simulating a real
Kenyan hospital system export. Injects 20+ categories of data quality
issues across all tables.

Tables produced:
  patients.csv        — master patient registry
  admissions.csv      — inpatient admissions & discharge records
  lab_tests.csv       — lab orders and results
  outpatient_visits.csv — outpatient clinic visits
"""

import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta
from pathlib import Path

random.seed(99)
np.random.seed(99)

OUT = Path("data")
OUT.mkdir(exist_ok=True)

# ── helpers ───────────────────────────────────────────────────────────────────

def rand_date(start, end):
    return start + timedelta(days=random.randint(0, (end - start).days))

def rand_dob(min_age=1, max_age=90):
    today = datetime(2024, 6, 30)
    days = random.randint(min_age * 365, max_age * 365)
    return today - timedelta(days=days)

def corrupt(value, bad_choices, prob=0.07):
    return random.choice(bad_choices) if random.random() < prob else value

def maybe_null(value, prob=0.06):
    return None if random.random() < prob else value

def messy_str(s, prob=0.08):
    if random.random() < prob:
        choice = random.choice(["upper", "lower", "mixed", "extra_space"])
        if choice == "upper":   return str(s).upper()
        if choice == "lower":   return str(s).lower()
        if choice == "mixed":   return str(s).swapcase()
        if choice == "extra_space": return "  " + str(s) + "  "
    return s

# ── 1. PATIENTS ───────────────────────────────────────────────────────────────

N_PATIENTS = 400

FIRST_NAMES = ["Amina","Brian","Carol","David","Elizabeth","Francis","Grace",
               "Henry","Irene","James","Kendi","Lilian","Moses","Naomi",
               "Oscar","Patricia","Quentin","Ruth","Samuel","Tabitha",
               "Umar","Violet","Walter","Xena","Yusuf","Zipporah"]
LAST_NAMES  = ["Mwangi","Otieno","Kamau","Wanjiku","Odhiambo","Kimani",
               "Njoroge","Auma","Mugo","Chebet","Maina","Korir","Ndung'u",
               "Owino","Gathoni","Rotich","Karanja","Achieng","Mutua","Waweru"]
COUNTIES    = ["Nairobi","Mombasa","Kisumu","Nakuru","Eldoret","Thika",
               "Machakos","Nyeri","Meru","Kakamega"]
BLOOD_GRPS  = ["A+","A-","B+","B-","O+","O-","AB+","AB-"]
GENDERS     = ["Male","Female"]

patients = []
used_ids = set()
for i in range(1, N_PATIENTS + 1):
    pid = f"PAT-{i:04d}"
    fn  = random.choice(FIRST_NAMES)
    ln  = random.choice(LAST_NAMES)
    dob = rand_dob()

    # Inject issues
    gender = messy_str(random.choice(GENDERS), prob=0.10)
    gender = corrupt(gender, ["M","F","male","FEMALE","Unknown","N/A",""], prob=0.06)

    blood = maybe_null(random.choice(BLOOD_GRPS), prob=0.10)
    blood = corrupt(blood, ["A","B","AB","O","unknown","?"], prob=0.05)

    phone = f"+2547{random.randint(10000000,99999999)}"
    phone = corrupt(phone, ["N/A","","07123","not provided","00000000000"], prob=0.07)

    county = messy_str(random.choice(COUNTIES), prob=0.09)
    county = maybe_null(county, prob=0.05)

    # DOB issues
    if random.random() < 0.08:
        dob_str = random.choice(["N/A","unknown","13/25/1990","00/00/0000","TBD",None])
    else:
        dob_str = dob.strftime(random.choice(["%d/%m/%Y","%Y-%m-%d","%m-%d-%Y"]))

    # Duplicate patient (same name, different ID)
    if random.random() < 0.04 and patients:
        dup = random.choice(patients)
        fn, ln = dup["first_name"], dup["last_name"]

    patients.append({
        "patient_id":   corrupt(pid, [None, f"PAT{i:04d}", f"P-{i}", ""], prob=0.03),
        "first_name":   messy_str(fn, prob=0.07),
        "last_name":    messy_str(ln, prob=0.07),
        "date_of_birth":dob_str,
        "gender":       gender,
        "blood_group":  blood,
        "county":       county,
        "phone":        phone,
        "registration_date": rand_date(datetime(2018,1,1), datetime(2024,1,1)).strftime("%Y-%m-%d"),
        "is_active":    corrupt(random.choice([1,1,1,0]), ["yes","no","TRUE","FALSE","1.0"], prob=0.06),
    })

df_patients = pd.DataFrame(patients)
# inject full-row duplicates
df_patients = pd.concat([df_patients, df_patients.sample(18, random_state=7)], ignore_index=True)
df_patients.sample(frac=1, random_state=1).reset_index(drop=True).to_csv(OUT/"patients.csv", index=False)
print(f"patients.csv:          {len(df_patients)} rows")

# ── 2. ADMISSIONS ─────────────────────────────────────────────────────────────

N_ADM = 600
WARDS       = ["General","ICU","Maternity","Paediatrics","Surgical","Orthopaedics","Oncology"]
DIAGNOSES   = ["Malaria","Typhoid","Pneumonia","Diabetes Mellitus","Hypertension",
               "Road Traffic Accident","Appendicitis","Tuberculosis","HIV/AIDS",
               "Acute Gastroenteritis","Anaemia","Preeclampsia","Fracture","Sepsis"]
DISCHARGE   = ["Recovered","Referred","Deceased","Absconded","Against Medical Advice"]
DOCTORS     = ["Dr. Kariuki","Dr. Omondi","Dr. Wanjiku","Dr. Muthoni","Dr. Chebet","Dr. Otieno"]

admissions = []
for i in range(1, N_ADM + 1):
    adm_id  = f"ADM-{i:05d}"
    pid     = f"PAT-{random.randint(1, N_PATIENTS):04d}"
    ward    = messy_str(random.choice(WARDS), prob=0.09)
    diag    = maybe_null(random.choice(DIAGNOSES), prob=0.07)
    doc     = messy_str(maybe_null(random.choice(DOCTORS), prob=0.06), prob=0.08)

    adm_dt  = rand_date(datetime(2022,1,1), datetime(2024,6,1))
    stay    = random.randint(1, 30)
    dis_dt  = adm_dt + timedelta(days=stay)

    # Date issues
    if random.random() < 0.07:
        adm_str = random.choice(["N/A","TBD","unknown",None,"32/01/2023"])
    else:
        adm_str = adm_dt.strftime(random.choice(["%d/%m/%Y","%Y-%m-%d"]))

    if random.random() < 0.06:
        dis_str = random.choice([None,"N/A","pending","TBD"])
    elif random.random() < 0.04:
        dis_str = (adm_dt - timedelta(days=2)).strftime("%Y-%m-%d")  # discharge BEFORE admission
    else:
        dis_str = dis_dt.strftime("%Y-%m-%d")

    # Cost issues
    cost = round(random.uniform(2000, 150000), 2)
    cost = corrupt(cost, [None, -999, 0, "N/A", "waived", 9999999], prob=0.07)

    outcome = maybe_null(random.choice(DISCHARGE), prob=0.06)
    outcome = corrupt(outcome, ["Dead","alive","RECOVERED","ref","AMA"], prob=0.06)

    admissions.append({
        "admission_id":   adm_id,
        "patient_id":     corrupt(pid, [None, f"PAT{random.randint(1,999):04d}", "UNKNOWN"], prob=0.04),
        "admission_date": adm_str,
        "discharge_date": dis_str,
        "ward":           ward,
        "diagnosis":      diag,
        "attending_doctor": doc,
        "total_cost_kes": cost,
        "discharge_outcome": outcome,
        "bed_number":     maybe_null(random.randint(1, 120), prob=0.08),
    })

df_adm = pd.DataFrame(admissions)
df_adm = pd.concat([df_adm, df_adm.sample(20, random_state=3)], ignore_index=True)
df_adm.sample(frac=1, random_state=2).reset_index(drop=True).to_csv(OUT/"admissions.csv", index=False)
print(f"admissions.csv:        {len(df_adm)} rows")

# ── 3. LAB TESTS ──────────────────────────────────────────────────────────────

N_LABS = 1200
TESTS = {
    "Full Blood Count":       ("FBC",   "cells/µL",  4000, 11000),
    "Malaria RDT":            ("MAL",   "N/A",        0,    1),
    "Blood Glucose (Fasting)":("GLU",   "mmol/L",    3.9,  7.0),
    "CD4 Count":              ("CD4",   "cells/µL",  200, 1500),
    "Haemoglobin":            ("HGB",   "g/dL",       7,   18),
    "Creatinine":             ("CRE",   "µmol/L",    53,  120),
    "ALT (Liver)":            ("ALT",   "U/L",        7,   56),
    "Urine Analysis":         ("URI",   "N/A",        0,    1),
    "HIV Rapid Test":         ("HIV",   "N/A",        0,    1),
    "Typhoid (Widal)":        ("WID",   "titre",      1,  320),
}
TEST_NAMES = list(TESTS.keys())
RESULT_STATUSES = ["Normal","Abnormal","Critical","Borderline"]

labs = []
for i in range(1, N_LABS + 1):
    lab_id   = f"LAB-{i:06d}"
    pid      = f"PAT-{random.randint(1, N_PATIENTS):04d}"
    adm_id   = maybe_null(f"ADM-{random.randint(1, N_ADM):05d}", prob=0.35)  # many outpatient labs
    test     = random.choice(TEST_NAMES)
    code, unit, lo, hi = TESTS[test]

    val = round(random.uniform(lo * 0.5, hi * 1.8), 2)
    val = corrupt(val, [None, "N/A", "pending", "error", -1, 99999], prob=0.07)

    ref_range = f"{lo} - {hi}"
    status = maybe_null(random.choice(RESULT_STATUSES), prob=0.07)
    status = corrupt(status, ["normal","ABNORMAL","Crit","borderline","N/A"], prob=0.06)

    ordered_dt = rand_date(datetime(2022,1,1), datetime(2024,6,1))
    if random.random() < 0.07:
        result_dt = None
    elif random.random() < 0.04:
        result_dt = ordered_dt - timedelta(days=1)  # result before order — impossible
    else:
        result_dt = ordered_dt + timedelta(hours=random.randint(1, 72))

    labs.append({
        "lab_id":         lab_id,
        "patient_id":     corrupt(pid, [None, "UNKNOWN", f"PAT{random.randint(1,999)}"], prob=0.04),
        "admission_id":   adm_id,
        "test_name":      messy_str(test, prob=0.08),
        "test_code":      code,
        "ordered_date":   ordered_dt.strftime(random.choice(["%d/%m/%Y","%Y-%m-%d"])),
        "result_date":    result_dt.strftime("%Y-%m-%d") if result_dt else None,
        "result_value":   val,
        "unit":           maybe_null(unit, prob=0.06),
        "reference_range": maybe_null(ref_range, prob=0.08),
        "result_status":  status,
        "technician":     maybe_null(f"Tech-{random.randint(1,15)}", prob=0.05),
    })

df_labs = pd.DataFrame(labs)
df_labs = pd.concat([df_labs, df_labs.sample(30, random_state=5)], ignore_index=True)
df_labs.sample(frac=1, random_state=3).reset_index(drop=True).to_csv(OUT/"lab_tests.csv", index=False)
print(f"lab_tests.csv:         {len(df_labs)} rows")

# ── 4. OUTPATIENT VISITS ──────────────────────────────────────────────────────

N_OPD = 900
CLINICS   = ["General OPD","Antenatal Care","Dental","Eye","HIV/AIDS Clinic",
             "Diabetes Clinic","Paediatric OPD","TB Clinic","Nutrition"]
COMPLAINTS= ["Fever","Headache","Chest Pain","Abdominal Pain","Joint Pain",
             "Cough","Fatigue","Skin Rash","Vomiting","Back Pain","Dizziness"]
OUTCOMES_OPD = ["Prescription given","Referred to specialist","Admitted",
                "Investigations ordered","Counselled & discharged","Follow-up scheduled"]

opd = []
for i in range(1, N_OPD + 1):
    opd_id = f"OPD-{i:06d}"
    pid    = f"PAT-{random.randint(1, N_PATIENTS):04d}"
    clinic = messy_str(random.choice(CLINICS), prob=0.09)
    complaint = maybe_null(random.choice(COMPLAINTS), prob=0.07)
    outcome   = maybe_null(random.choice(OUTCOMES_OPD), prob=0.06)
    outcome   = corrupt(outcome, ["admitted","REFERRED","given Rx","N/A","discharged"], prob=0.06)

    visit_dt  = rand_date(datetime(2022,1,1), datetime(2024,6,30))
    if random.random() < 0.07:
        visit_str = random.choice([None,"N/A","TBD","00/00/0000"])
    else:
        visit_str = visit_dt.strftime(random.choice(["%d/%m/%Y","%Y-%m-%d","%d-%m-%Y"]))

    wait  = random.randint(5, 240)
    wait  = corrupt(wait, [None, -10, 0, 9999, "N/A"], prob=0.06)

    fee   = round(random.uniform(100, 3000), 2)
    fee   = corrupt(fee, [None, -50, 0, "waived", "N/A"], prob=0.07)

    doctor = messy_str(maybe_null(random.choice(DOCTORS), prob=0.07), prob=0.08)

    opd.append({
        "visit_id":       opd_id,
        "patient_id":     corrupt(pid, [None, "UNKNOWN", f"PAT{random.randint(1,999)}"], prob=0.04),
        "visit_date":     visit_str,
        "clinic":         clinic,
        "chief_complaint":complaint,
        "attending_doctor": doctor,
        "wait_time_mins": wait,
        "consultation_fee_kes": fee,
        "outcome":        outcome,
        "follow_up_date": maybe_null((visit_dt + timedelta(days=random.randint(7,90))).strftime("%Y-%m-%d"), prob=0.45),
    })

df_opd = pd.DataFrame(opd)
df_opd = pd.concat([df_opd, df_opd.sample(25, random_state=6)], ignore_index=True)
df_opd.sample(frac=1, random_state=4).reset_index(drop=True).to_csv(OUT/"outpatient_visits.csv", index=False)
print(f"outpatient_visits.csv: {len(df_opd)} rows")

print("\n✅ All datasets generated in ./data/")
