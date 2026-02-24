"""
User Guide: unified_event_table.parquet
=======================================
Quick examples for common multimodal analysis tasks.
Requires: pip install duckdb
"""

import duckdb
from pathlib import Path

TABLE = str(Path(__file__).parent / "unified_event_table.parquet")
conn = duckdb.connect()


def show(query, msg=None):
    """Run a query, print the result, and return the dataframe.
    To save: df.to_csv("output.csv", index=False)"""
    df = conn.execute(query).fetchdf()
    if msg:
        print(f"\n{msg}")
    print(df.to_string(index=False))
    print(f"  [{len(df)} rows]")
    return df


# ============================================================
# 1. Overview — what's in the table?
# ============================================================

df = show(f"""
    SELECT modality, COUNT(*) AS rows, COUNT(DISTINCT subject_id) AS patients
    FROM '{TABLE}'
    GROUP BY modality ORDER BY rows DESC
""", msg="Table overview — rows and patients per modality:")

# df.to_csv("overview.csv", index=False)

# ============================================================
# 2. Multimodal timeline of a single patient
# ============================================================
# >>> Set your patient ID here <<<
PATIENT_ID = 10074611

# What modalities does this patient have?
df = show(f"""
    SELECT modality, COUNT(*) AS events,
           MIN(event_time) AS first_event, MAX(event_time) AS last_event
    FROM '{TABLE}'
    WHERE subject_id = {PATIENT_ID}
    GROUP BY modality ORDER BY first_event
""", msg=f"Patient {PATIENT_ID} — modality summary:")

# Chronological timeline
df = show(f"""
    SELECT event_time, modality, location_type, item_labels, file_path, study_id
    FROM '{TABLE}'
    WHERE subject_id = {PATIENT_ID}
    ORDER BY event_time
    LIMIT 20
""", msg=f"Patient {PATIENT_ID} — chronological timeline:")
# df.to_csv("patient_timeline.csv", index=False)

# ============================================================
# 3. Extract specific lab/vital values from LIST columns
# ============================================================
# >>> Set your patient IDs here <<<
PATIENT_IDS = [10074611, 10002155]
patient_ids_str = ", ".join(str(p) for p in PATIENT_IDS)

# Example: Get creatinine values for a list of patients
df = show(f"""
    SELECT subject_id, event_time, label, value
    FROM (
        SELECT subject_id, event_time,
               UNNEST(item_labels) AS label,
               UNNEST("values") AS value
        FROM '{TABLE}'
        WHERE subject_id IN ({patient_ids_str}) AND modality = 'lab'
    )
    WHERE label = 'Creatinine'
    ORDER BY subject_id, event_time
    LIMIT 20
""", msg=f"Creatinine values for patients {PATIENT_IDS}:")
# df.to_csv("creatinine.csv", index=False)

# Example: Get heart rate and NIBP systolic from vitals
df = show(f"""
    SELECT subject_id, event_time, label, value
    FROM (
        SELECT subject_id, event_time,
               UNNEST(item_labels) AS label,
               UNNEST("values") AS value
        FROM '{TABLE}'
        WHERE subject_id IN ({patient_ids_str}) AND modality = 'vital'
    )
    WHERE label IN ('Heart Rate', 'Non Invasive Blood Pressure systolic')
    ORDER BY subject_id, event_time, label
    LIMIT 30
""", msg=f"Heart Rate & NIBP systolic for patients {PATIENT_IDS}:")
# df.to_csv("vitals_extracted.csv", index=False)


# ============================================================
# 4. USE CASE: Match two modalities within a time window
# ============================================================

# Available modalities: lab, vital, echo_measurement, cxr, ecg, echo, radiology_note, discharge_note
# Change these to match any two modalities
MODALITY_A = 'ecg'
MODALITY_B = 'echo'
WINDOW_DAYS = 30

# Example: closest pair (either direction) within WINDOW_DAYS

df = show(f"""
    WITH pairs AS (
        SELECT
            a.subject_id,
            a.event_time AS {MODALITY_A}_time,
            a.study_id   AS {MODALITY_A}_study_id,
            a.file_path  AS {MODALITY_A}_path,
            b.event_time AS {MODALITY_B}_time,
            b.study_id   AS {MODALITY_B}_study_id,
            b.file_path  AS {MODALITY_B}_path,
            EPOCH(a.event_time - b.event_time) / 86400 AS days_apart,  -- positive = A after B
            ROW_NUMBER() OVER (
                PARTITION BY a.subject_id, a.event_time
                ORDER BY ABS(EPOCH(a.event_time - b.event_time))
            ) AS rn
        FROM '{TABLE}' a
        JOIN '{TABLE}' b
            ON a.subject_id = b.subject_id
            AND ABS(EPOCH(a.event_time - b.event_time)) <= {WINDOW_DAYS} * 86400
        WHERE a.modality = '{MODALITY_A}'
          AND b.modality = '{MODALITY_B}'
    )
    SELECT subject_id,
           {MODALITY_A}_time, {MODALITY_A}_study_id, {MODALITY_A}_path,
           {MODALITY_B}_time, {MODALITY_B}_study_id, {MODALITY_B}_path,
           ROUND(days_apart, 1) AS days_apart
    FROM pairs WHERE rn = 1
    LIMIT 10
""", msg=f"{MODALITY_A.upper()} + {MODALITY_B.upper()} within {WINDOW_DAYS} days (closest pair):")
# df.to_csv("ecg_echo_pairs.csv", index=False)

# Example: directional matching — B must precede A (e.g. CXR before ECG within 7 days)
MODALITY_A = 'ecg'
MODALITY_B = 'cxr'
WINDOW_DAYS = 7

df = show(f"""
    WITH pairs AS (
        SELECT
            a.subject_id,
            b.event_time AS {MODALITY_B}_time,
            b.study_id   AS {MODALITY_B}_study_id,
            b.file_path  AS {MODALITY_B}_path,
            a.event_time AS {MODALITY_A}_time,
            a.study_id   AS {MODALITY_A}_study_id,
            a.file_path  AS {MODALITY_A}_path,
            EPOCH(a.event_time - b.event_time) / 86400 AS days_after_{MODALITY_B},
            ROW_NUMBER() OVER (
                PARTITION BY a.subject_id, a.event_time
                ORDER BY EPOCH(a.event_time - b.event_time)
            ) AS rn
        FROM '{TABLE}' a
        JOIN '{TABLE}' b
            ON a.subject_id = b.subject_id
            AND b.event_time < a.event_time                              -- B must come first
            AND EPOCH(a.event_time - b.event_time) <= {WINDOW_DAYS} * 86400
        WHERE a.modality = '{MODALITY_A}'
          AND b.modality = '{MODALITY_B}'
    )
    SELECT subject_id,
           {MODALITY_B}_time, {MODALITY_B}_study_id, {MODALITY_B}_path,
           {MODALITY_A}_time, {MODALITY_A}_study_id, {MODALITY_A}_path,
           ROUND(days_after_{MODALITY_B}, 1) AS days_after_{MODALITY_B}
    FROM pairs WHERE rn = 1
    LIMIT 10
""", msg=f"{MODALITY_B.upper()} preceding {MODALITY_A.upper()} within {WINDOW_DAYS} days (closest {MODALITY_B.upper()} before each {MODALITY_A.upper()}):")

# df.to_csv("cxr_before_ecg_pairs.csv", index=False)


# ============================================================
# 5. USE CASE: Condition-based cohort
#    Get multimodal data for patients with specific ICD codes
# ============================================================

# >>> CONFIG: Set all parameters here <<<

# ICD code prefixes (pneumonia example: ICD-9 480-486, ICD-10 J12-J18)
ICD9_PREFIXES  = ['480', '481', '482', '483', '484', '485', '486']
ICD10_PREFIXES = ['J12', 'J13', 'J14', 'J15', 'J16', 'J17', 'J18']

# Modalities to extract for the cohort
# Available: lab, vital, echo_measurement, cxr, ecg, echo, radiology_note, discharge_note
COHORT_MODALITIES = ['cxr', 'ecg', 'vital', 'lab']

# MIMIC-IV diagnoses source (set one, leave the other as None)
MIMIC_IV_DUCKDB = Path("/Users/ahramhan/my-research/m4_data/databases/mimic_iv.duckdb")
MIMIC_IV_PATH   = None  # e.g. Path("/data/physionet/mimic-iv-3.1")

# --- derived strings (no need to edit below) ---
modalities_str = ", ".join(f"'{m}'" for m in COHORT_MODALITIES)
icd9_like  = " OR ".join(f"icd_code LIKE '{c}%%'" for c in ICD9_PREFIXES)
icd10_like = " OR ".join(f"icd_code LIKE '{c}%%'" for c in ICD10_PREFIXES)

# Resolve diagnoses source
attached_duckdb = False
if MIMIC_IV_DUCKDB and MIMIC_IV_DUCKDB.exists():
    conn.execute(f"ATTACH '{MIMIC_IV_DUCKDB}' AS mimic (READ_ONLY)")
    attached_duckdb = True
    diagnoses_src = "mimic.mimiciv_hosp.diagnoses_icd"
elif MIMIC_IV_PATH:
    diagnoses_src = f"read_csv_auto('{MIMIC_IV_PATH}/hosp/diagnoses_icd.csv.gz')"
else:
    raise FileNotFoundError("No MIMIC-IV diagnoses source found. Set MIMIC_IV_DUCKDB or MIMIC_IV_PATH above.")

df = show(f"""
    WITH cohort AS (
        SELECT DISTINCT subject_id, hadm_id
        FROM {diagnoses_src}
        WHERE (icd_version = 9 AND ({icd9_like}))
           OR (icd_version = 10 AND ({icd10_like}))
    )
    SELECT e.subject_id, e.hadm_id, e.event_time, e.modality,
           e.location_type, e.item_ids, e.item_labels, e."values", e.file_path
    FROM '{TABLE}' e
    JOIN cohort c ON e.subject_id = c.subject_id AND e.hadm_id = c.hadm_id
    WHERE e.modality IN ({modalities_str})
    ORDER BY e.subject_id, e.event_time
    LIMIT 20
""", msg="Condition-based cohort — multimodal events:")
if attached_duckdb:
    conn.execute("DETACH mimic")
# df.to_csv("condition_cohort.csv", index=False)


# ============================================================
# 6. Filter by care location
# ============================================================

df = show(f"""
    SELECT subject_id, event_time, careunit, item_labels, "values"
    FROM '{TABLE}'
    WHERE modality = 'vital' AND location_type = 'ICU'
    LIMIT 5
""", msg="ICU vital events (first 5):")
# df.to_csv("icu_vitals.csv", index=False)