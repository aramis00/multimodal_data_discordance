"""
Create a unified long-format event table integrating MIMIC-IV multimodal data.

Purpose:
    Consolidate heterogeneous clinical data sources into a single time-indexed
    event table for multimodal data discordance analysis.

Modalities (8 types):
    Structured (numeric, hourly aggregated with LIST columns):
        - lab             : 50 major blood lab items from hosp/labevents
                            (Blood Gas, Chemistry, Hematology)
        - vital           : 11 vital sign items from icu/chartevents
                            (HR, BP, RR, Temp, SpO2, CVP)
        - echo_measurement: Structured echo machine values from mimic-iv-echo-note
                            (e.g. LVEF, E/A ratio, wall thickness)
    Imaging (event markers with file_path/study_id):
        - cxr             : Chest X-ray studies from mimic-cxr-jpg metadata
        - ecg             : 12-lead ECG records from mimic-iv-ecg
        - echo            : Echocardiogram DICOM studies from mimic-iv-echo
    Clinical notes (event markers only, no text; join by note_id):
        - radiology_note  : Radiology reports from mimic-iv-note
        - discharge_note  : Discharge summaries from mimic-iv-note

Schema:
    subject_id     BIGINT       Patient ID
    hadm_id        BIGINT       Hospital admission ID (NULL if unmatched)
    event_time     TIMESTAMP    Event timestamp (hourly-truncated for structured)
    modality       VARCHAR      One of the 8 modality types above
    location_type  VARCHAR      ER / ICU / Ward / Unknown (from transfers)
    careunit       VARCHAR      Specific care unit name
    item_ids       BIGINT[]     Item IDs (lab/vital only)
    item_labels    VARCHAR[]    Item names or note_type
    values         DOUBLE[]     Numeric values (lab/vital/echo_measurement only)
    file_path      VARCHAR      Relative path to source file (imaging only)
    study_id       BIGINT       Study or note ID

Processing pipeline:
    Step 1   : Build location lookup from hosp/transfers
    Step 2   : Load admissions for hadm_id matching
    Step 3a-h: Extract each modality (hourly median -> LIST aggregation for structured)
    Step 4   : Fill hadm_id for imaging/echo_meas via admissions time window
    Step 5   : ASOF JOIN location from transfers
    Step 6   : Location join for large tables (lab, vital) via parquet
    Step 7   : UNION ALL -> export Parquet

Data sources:
    mimic-iv-3.1            hosp/labevents, hosp/admissions, hosp/transfers,
                            icu/chartevents
    mimic-cxr-jpg-2.1       mimic-cxr-2.0.0-metadata.csv.gz
    mimic-iv-ecg (1.0)      record_list.csv
    mimic-iv-echo (0.1)     echo-study-list.csv
    mimic-iv-echo-note      structured_measurements.csv.gz
    mimic-iv-note (2.2)     radiology.csv.gz, discharge.csv.gz

Output:
    unified_event_table.parquet   All 8 modalities (Parquet, ZSTD compressed)
"""

import duckdb
from pathlib import Path
import shutil

# ──────────────────────────────────────────────────────
# Paths
# ──────────────────────────────────────────────────────
PHYSIONET = Path("***************")
MIMIC_IV = PHYSIONET / "mimic-iv-3.1"
MIMIC_CXR_META = PHYSIONET / "mimic-cxr-jpg-2.1" / "mimic-cxr-2.0.0-metadata.csv.gz"
MIMIC_ECG = PHYSIONET / "mimic-iv-ecg-diagnostic-electrocardiogram-matched-subset-1.0"
MIMIC_ECHO = PHYSIONET / "mimic-iv-echo" / "0.1"
MIMIC_ECHO_NOTE = PHYSIONET / "mimic-iv-echo-note"
MIMIC_NOTE = PHYSIONET / "mimic-iv-note" / "2.2" / "note"

OUTPUT_DIR = Path("***************/multimodal_data_discordance/data")
OUTPUT_ALL = OUTPUT_DIR / "unified_event_table.parquet"

# ──────────────────────────────────────────────────────
# Major lab item IDs (Blood only, 50 items)
# ──────────────────────────────────────────────────────
MAJOR_LAB_ITEMIDS = [
    # Blood Gas
    50802,  # Base Excess
    50813,  # Lactate
    50818,  # pCO2
    50820,  # pH
    50821,  # pO2
    # Chemistry (Blood)
    50861,  # ALT
    50862,  # Albumin
    50863,  # Alkaline Phosphatase
    50867,  # Amylase
    50882,  # Bicarbonate
    50883,  # Bilirubin, Direct
    50885,  # Bilirubin, Total
    50889,  # C-Reactive Protein
    50893,  # Calcium, Total
    50902,  # Chloride
    50908,  # CK-MB Index
    50912,  # Creatinine
    50924,  # Ferritin
    50931,  # Glucose
    50952,  # Iron
    50954,  # Lactate Dehydrogenase (LD)
    50956,  # Lipase
    50960,  # Magnesium
    50963,  # NTproBNP
    50970,  # Phosphate
    50971,  # Potassium
    50976,  # Protein, Total
    50983,  # Sodium
    51002,  # Troponin I
    51003,  # Troponin T
    51006,  # Urea Nitrogen
    50878,  # Aspartate Aminotransferase (AST)
    50868,  # Anion Gap
    50910,  # Creatine Kinase (CK)
    50993,  # Thyroid Stimulating Hormone (TSH)
    50852,  # Hemoglobin A1c (HbA1c)
    51007,  # Uric Acid
    50915,  # D-Dimer
    50927,  # Gamma Glutamyltransferase (GGT)
    # Hematology (Blood)
    51221,  # Hematocrit
    51222,  # Hemoglobin
    51237,  # INR(PT)
    51248,  # MCH
    51249,  # MCHC
    51250,  # MCV
    51265,  # Platelet Count
    51274,  # PT
    51275,  # PTT
    51277,  # RDW
    51301,  # White Blood Cells
]

# ──────────────────────────────────────────────────────
# Major vital sign item IDs (from chartevents / ICU)
# ──────────────────────────────────────────────────────
MAJOR_VITAL_ITEMIDS = [
    220045,   # Heart Rate
    220179,   # Non Invasive Blood Pressure systolic
    220180,   # Non Invasive Blood Pressure diastolic
    220050,   # Arterial Blood Pressure systolic
    220051,   # Arterial Blood Pressure diastolic
    220052,   # Arterial Blood Pressure mean
    220210,   # Respiratory Rate
    223761,   # Temperature Fahrenheit
    220277,   # O2 saturation pulseoxymetry (SpO2)
    220074,   # Central Venous Pressure
    224690,   # Respiratory Rate (Total)
]

# ──────────────────────────────────────────────────────
# ICU careunit names for location classification
# ──────────────────────────────────────────────────────
ICU_UNITS = [
    'Medical Intensive Care Unit (MICU)',
    'Medical/Surgical Intensive Care Unit (MICU/SICU)',
    'Surgical Intensive Care Unit (SICU)',
    'Cardiac Vascular Intensive Care Unit (CVICU)',
    'Coronary Care Unit (CCU)',
    'Neuro Surgical Intensive Care Unit (Neuro SICU)',
    'Trauma SICU (TSICU)',
]


def _batched_list_agg(
    conn, input_pq, output_pq, parts_dir,
    modality, id_col, label_col, value_col, has_hadm,
    batch_size=50_000,
):
    """list() aggregation in subject_id batches to avoid OOM."""
    parts_dir = Path(parts_dir)
    if parts_dir.exists():
        shutil.rmtree(parts_dir)
    parts_dir.mkdir(parents=True)

    subjects = [r[0] for r in conn.execute(
        f"SELECT DISTINCT subject_id FROM '{input_pq}' ORDER BY subject_id"
    ).fetchall()]

    group_cols = "subject_id, hadm_id, event_time" if has_hadm else "subject_id, event_time"
    hadm_select = "hadm_id," if has_hadm else "NULL::BIGINT AS hadm_id,"
    id_list = f"list({id_col} ORDER BY {id_col}) AS item_ids," if id_col else "NULL::BIGINT[] AS item_ids,"
    order_col = id_col if id_col else label_col

    for i in range(0, len(subjects), batch_size):
        batch_subs = subjects[i:i + batch_size]
        sub_lo, sub_hi = batch_subs[0], batch_subs[-1]
        part_path = str(parts_dir / f"part_{i:06d}.parquet")

        conn.execute(f"""
            COPY (
                SELECT
                    subject_id,
                    {hadm_select}
                    event_time,
                    '{modality}' AS modality,
                    {id_list}
                    list({label_col} ORDER BY {order_col}) AS item_labels,
                    list({value_col} ORDER BY {order_col}) AS "values",
                    NULL AS file_path,
                    NULL::BIGINT AS study_id
                FROM '{input_pq}'
                WHERE subject_id BETWEEN {sub_lo} AND {sub_hi}
                GROUP BY {group_cols}
            ) TO '{part_path}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """)
        done = min(i + batch_size, len(subjects))
        print(f"    batch {done:,}/{len(subjects):,} subjects")

    # Merge all parts
    conn.execute(f"""
        COPY (
            SELECT * FROM '{parts_dir / '*.parquet'}'
        ) TO '{output_pq}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """)
    shutil.rmtree(parts_dir, ignore_errors=True)


def main():
    TMP_DIR = OUTPUT_DIR / ".tmp"
    TMP_DIR.mkdir(exist_ok=True)

    conn = duckdb.connect(str(TMP_DIR / "workspace.duckdb"))
    conn.execute("SET threads TO 4")
    conn.execute("SET memory_limit = '8GB'")
    conn.execute("SET preserve_insertion_order = false")
    conn.execute(f"SET temp_directory = '{TMP_DIR}'")
    conn.execute("PRAGMA enable_progress_bar")

    itemid_list = ", ".join(str(i) for i in MAJOR_LAB_ITEMIDS)
    vital_itemid_list = ", ".join(str(i) for i in MAJOR_VITAL_ITEMIDS)
    icu_list = ", ".join(f"'{u}'" for u in ICU_UNITS)

    # ──────────────────────────────────────────────────
    # Step 1: Location lookup from transfers
    # ──────────────────────────────────────────────────
    print("Step 1: Building location lookup from transfers...")
    conn.execute(f"""
        CREATE OR REPLACE TABLE location_lookup AS
        SELECT
            subject_id,
            hadm_id,
            intime::TIMESTAMP AS intime,
            outtime::TIMESTAMP AS outtime,
            careunit,
            CASE
                WHEN careunit LIKE '%Emergency%' THEN 'ER'
                WHEN careunit IN ({icu_list}) THEN 'ICU'
                WHEN careunit IS NULL THEN 'Unknown'
                ELSE 'Ward'
            END AS location_type
        FROM read_csv_auto('{MIMIC_IV / "hosp" / "transfers.csv.gz"}')
        WHERE eventtype != 'discharge'
    """)
    n = conn.execute("SELECT COUNT(*) FROM location_lookup").fetchone()[0]
    print(f"  -> {n:,} transfer records loaded")

    # ──────────────────────────────────────────────────
    # Step 2: Admissions (for hadm_id matching)
    # ──────────────────────────────────────────────────
    print("Step 2: Loading admissions...")
    conn.execute(f"""
        CREATE OR REPLACE TABLE admissions AS
        SELECT
            subject_id,
            hadm_id,
            admittime::TIMESTAMP AS admittime,
            dischtime::TIMESTAMP AS dischtime
        FROM read_csv_auto('{MIMIC_IV / "hosp" / "admissions.csv.gz"}')
    """)
    n = conn.execute("SELECT COUNT(*) FROM admissions").fetchone()[0]
    print(f"  -> {n:,} admissions loaded")

    # ──────────────────────────────────────────────────
    # Step 3a: Lab events (major items, hourly grouped)
    #   Pass 1: hourly median per item -> temp parquet
    #   Pass 2: list aggregation from parquet
    # ──────────────────────────────────────────────────
    lab_median_tmp = str(TMP_DIR / "lab_hourly_median.parquet")
    print("Step 3a-1: Lab hourly median per item -> parquet...")
    conn.execute(f"""
        COPY (
            SELECT
                le.subject_id,
                le.hadm_id,
                date_trunc('hour', le.charttime::TIMESTAMP) AS event_time,
                le.itemid,
                di.label AS item_label,
                MEDIAN(le.valuenum) AS median_value
            FROM read_csv_auto('{MIMIC_IV / "hosp" / "labevents.csv.gz"}') le
            INNER JOIN read_csv_auto('{MIMIC_IV / "hosp" / "d_labitems.csv.gz"}') di
                ON le.itemid = di.itemid
            WHERE le.itemid IN ({itemid_list})
              AND le.valuenum IS NOT NULL
            GROUP BY le.subject_id, le.hadm_id,
                     date_trunc('hour', le.charttime::TIMESTAMP),
                     le.itemid, di.label
        ) TO '{lab_median_tmp}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """)
    n = conn.execute(f"SELECT COUNT(*) FROM '{lab_median_tmp}'").fetchone()[0]
    print(f"  -> {n:,} hourly-item medians")

    lab_grouped_tmp = str(TMP_DIR / "lab_grouped.parquet")
    print("Step 3a-2: Lab list aggregation -> parquet (batched)...")
    _batched_list_agg(
        conn, lab_median_tmp, lab_grouped_tmp, TMP_DIR / "lab_parts",
        modality='lab',
        id_col='itemid', label_col='item_label', value_col='median_value',
        has_hadm=True,
    )
    n = conn.execute(f"SELECT COUNT(*) FROM '{lab_grouped_tmp}'").fetchone()[0]
    print(f"  -> {n:,} lab events (hourly grouped)")

    # ──────────────────────────────────────────────────
    # Step 3b: CXR events (study-level with timestamps)
    # ──────────────────────────────────────────────────
    print("Step 3b: Extracting CXR events...")
    conn.execute(f"""
        CREATE OR REPLACE TABLE cxr_events AS
        SELECT
            subject_id,
            NULL::BIGINT AS hadm_id,
            TRY(make_timestamp(
                (StudyDate::BIGINT / 10000)::BIGINT,
                ((StudyDate::BIGINT % 10000) / 100)::BIGINT,
                (StudyDate::BIGINT % 100)::BIGINT,
                LEAST(COALESCE((FLOOR(StudyTime) / 10000)::BIGINT, 0), 23),
                LEAST(COALESCE(((FLOOR(StudyTime)::BIGINT % 10000) / 100)::BIGINT, 0), 59),
                LEAST(COALESCE((FLOOR(StudyTime)::BIGINT % 100)::DOUBLE, 0.0), 59.0)
            )) AS event_time,
            'cxr' AS modality,
            NULL::BIGINT[] AS item_ids,
            [ViewPosition] AS item_labels,
            NULL::DOUBLE[] AS "values",
            'files/p' || (subject_id // 1000000)::VARCHAR
                || '/p' || subject_id::VARCHAR
                || '/s' || study_id::VARCHAR AS file_path,
            study_id
        FROM (
            SELECT subject_id, study_id,
                   MIN(StudyDate) AS StudyDate,
                   MIN(StudyTime) AS StudyTime,
                   FIRST(ViewPosition) AS ViewPosition
            FROM read_csv_auto('{MIMIC_CXR_META}', quote='"')
            GROUP BY subject_id, study_id
        )
    """)
    n = conn.execute("SELECT COUNT(*) FROM cxr_events").fetchone()[0]
    print(f"  -> {n:,} CXR studies extracted")

    # ──────────────────────────────────────────────────
    # Step 3c: Echo DICOM events (study-level)
    # ──────────────────────────────────────────────────
    print("Step 3c: Extracting Echo DICOM events...")
    conn.execute(f"""
        CREATE OR REPLACE TABLE echo_events AS
        SELECT
            subject_id,
            NULL::BIGINT AS hadm_id,
            study_datetime::TIMESTAMP AS event_time,
            'echo' AS modality,
            NULL::BIGINT[] AS item_ids,
            NULL::VARCHAR[] AS item_labels,
            NULL::DOUBLE[] AS "values",
            'files/p' || (subject_id // 1000000)::VARCHAR
                || '/p' || subject_id::VARCHAR
                || '/s' || study_id::VARCHAR AS file_path,
            study_id
        FROM read_csv_auto('{MIMIC_ECHO / "echo-study-list.csv"}')
    """)
    n = conn.execute("SELECT COUNT(*) FROM echo_events").fetchone()[0]
    print(f"  -> {n:,} Echo DICOM studies extracted")

    # ──────────────────────────────────────────────────
    # Step 3d: ECG events
    # ──────────────────────────────────────────────────
    print("Step 3d: Extracting ECG events...")
    conn.execute(f"""
        CREATE OR REPLACE TABLE ecg_events AS
        SELECT
            subject_id,
            NULL::BIGINT AS hadm_id,
            ecg_time::TIMESTAMP AS event_time,
            'ecg' AS modality,
            NULL::BIGINT[] AS item_ids,
            NULL::VARCHAR[] AS item_labels,
            NULL::DOUBLE[] AS "values",
            path AS file_path,
            study_id
        FROM read_csv_auto('{MIMIC_ECG / "record_list.csv"}')
    """)
    n = conn.execute("SELECT COUNT(*) FROM ecg_events").fetchone()[0]
    print(f"  -> {n:,} ECG records extracted")

    # ──────────────────────────────────────────────────
    # Step 3e: Vital signs (hourly grouped from chartevents)
    #   Pass 1: hourly median per item -> temp parquet
    #   Pass 2: list aggregation from parquet
    # ──────────────────────────────────────────────────
    vital_median_tmp = str(TMP_DIR / "vital_hourly_median.parquet")
    print("Step 3e-1: Vital hourly median per item -> parquet...")
    conn.execute(f"""
        COPY (
            SELECT
                ce.subject_id,
                ce.hadm_id,
                date_trunc('hour', ce.charttime::TIMESTAMP) AS event_time,
                ce.itemid,
                di.label AS item_label,
                MEDIAN(ce.valuenum) AS median_value
            FROM read_csv_auto('{MIMIC_IV / "icu" / "chartevents.csv.gz"}') ce
            INNER JOIN read_csv_auto('{MIMIC_IV / "icu" / "d_items.csv.gz"}') di
                ON ce.itemid = di.itemid
            WHERE ce.itemid IN ({vital_itemid_list})
              AND ce.valuenum IS NOT NULL
            GROUP BY ce.subject_id, ce.hadm_id,
                     date_trunc('hour', ce.charttime::TIMESTAMP),
                     ce.itemid, di.label
        ) TO '{vital_median_tmp}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """)
    n = conn.execute(f"SELECT COUNT(*) FROM '{vital_median_tmp}'").fetchone()[0]
    print(f"  -> {n:,} hourly-item medians")

    vital_grouped_tmp = str(TMP_DIR / "vital_grouped.parquet")
    print("Step 3e-2: Vital list aggregation -> parquet (batched)...")
    _batched_list_agg(
        conn, vital_median_tmp, vital_grouped_tmp, TMP_DIR / "vital_parts",
        modality='vital',
        id_col='itemid', label_col='item_label', value_col='median_value',
        has_hadm=True,
        batch_size=5_000,
    )
    n = conn.execute(f"SELECT COUNT(*) FROM '{vital_grouped_tmp}'").fetchone()[0]
    print(f"  -> {n:,} vital events (hourly grouped)")

    # ──────────────────────────────────────────────────
    # Step 3f: Echo machine measurements (structured_measurements)
    # ──────────────────────────────────────────────────
    echo_median_tmp = str(TMP_DIR / "echo_meas_hourly_median.parquet")
    print("Step 3f-1: Echo measurement hourly median -> parquet...")
    conn.execute(f"""
        COPY (
            SELECT
                sm.subject_id,
                date_trunc('hour', sm.measurement_datetime::TIMESTAMP) AS event_time,
                sm.measurement AS meas_name,
                MEDIAN(TRY_CAST(sm.result AS DOUBLE)) AS median_value
            FROM read_csv_auto('{MIMIC_ECHO_NOTE / "structured_measurements.csv.gz"}') sm
            GROUP BY sm.subject_id,
                     date_trunc('hour', sm.measurement_datetime::TIMESTAMP),
                     sm.measurement
        ) TO '{echo_median_tmp}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """)
    n = conn.execute(f"SELECT COUNT(*) FROM '{echo_median_tmp}'").fetchone()[0]
    print(f"  -> {n:,} hourly-measurement medians")

    echo_grouped_tmp = str(TMP_DIR / "echo_meas_grouped.parquet")
    print("Step 3f-2: Echo measurement list aggregation -> parquet (batched)...")
    _batched_list_agg(
        conn, echo_median_tmp, echo_grouped_tmp, TMP_DIR / "echo_meas_parts",
        modality='echo_measurement',
        id_col=None, label_col='meas_name', value_col='median_value',
        has_hadm=False,
        batch_size=5_000,
    )
    n = conn.execute(f"SELECT COUNT(*) FROM '{echo_grouped_tmp}'").fetchone()[0]
    print(f"  -> {n:,} echo measurement events (hourly grouped)")

    # ──────────────────────────────────────────────────
    # Step 3g: Radiology notes (event markers only, no text)
    # ──────────────────────────────────────────────────
    print("Step 3g: Extracting Radiology note events...")
    conn.execute(f"""
        CREATE OR REPLACE TABLE rad_note_events AS
        SELECT
            subject_id,
            hadm_id,
            charttime::TIMESTAMP AS event_time,
            'radiology_note' AS modality,
            NULL::BIGINT[] AS item_ids,
            [note_type] AS item_labels,
            NULL::DOUBLE[] AS "values",
            NULL AS file_path,
            note_id AS study_id
        FROM read_csv_auto('{MIMIC_NOTE / "radiology.csv.gz"}')
        WHERE charttime IS NOT NULL
    """)
    n = conn.execute("SELECT COUNT(*) FROM rad_note_events").fetchone()[0]
    print(f"  -> {n:,} radiology note events extracted")

    # ──────────────────────────────────────────────────
    # Step 3h: Discharge notes (event markers only, no text)
    # ──────────────────────────────────────────────────
    print("Step 3h: Extracting Discharge note events...")
    conn.execute(f"""
        CREATE OR REPLACE TABLE dc_note_events AS
        SELECT
            subject_id,
            hadm_id,
            charttime::TIMESTAMP AS event_time,
            'discharge_note' AS modality,
            NULL::BIGINT[] AS item_ids,
            [note_type] AS item_labels,
            NULL::DOUBLE[] AS "values",
            NULL AS file_path,
            note_id AS study_id
        FROM read_csv_auto('{MIMIC_NOTE / "discharge.csv.gz"}')
        WHERE charttime IS NOT NULL
    """)
    n = conn.execute("SELECT COUNT(*) FROM dc_note_events").fetchone()[0]
    print(f"  -> {n:,} discharge note events extracted")

    # ──────────────────────────────────────────────────
    # Step 4: Fill hadm_id for imaging/echo_meas (small tables)
    # ──────────────────────────────────────────────────
    print("Step 4: Filling hadm_id for CXR/ECG/Echo...")
    for tbl in ["cxr_events", "echo_events", "ecg_events"]:
        conn.execute(f"""
            CREATE OR REPLACE TABLE {tbl}_hadm AS
            SELECT
                e.subject_id,
                a.hadm_id,
                e.event_time, e.modality, e.item_ids,
                e.item_labels, e."values", e.file_path, e.study_id
            FROM {tbl} e
            LEFT JOIN admissions a
                ON e.subject_id = a.subject_id
                AND e.event_time BETWEEN a.admittime AND a.dischtime
        """)
        n = conn.execute(f"SELECT COUNT(*) FROM {tbl}_hadm").fetchone()[0]
        n_hadm = conn.execute(f"SELECT COUNT(hadm_id) FROM {tbl}_hadm").fetchone()[0]
        print(f"  {tbl}: {n:,} rows, {n_hadm:,} with hadm_id ({100*n_hadm/max(n,1):.1f}%)")

    # Echo measurements hadm_id fill -> parquet (medium size)
    echo_meas_hadm_tmp = str(TMP_DIR / "echo_meas_hadm.parquet")
    print("  Filling hadm_id for echo_meas_events -> parquet...")
    conn.execute(f"""
        COPY (
            SELECT
                e.subject_id,
                a.hadm_id,
                e.event_time, e.modality, e.item_ids,
                e.item_labels, e."values", e.file_path, e.study_id
            FROM '{echo_grouped_tmp}' e
            LEFT JOIN admissions a
                ON e.subject_id = a.subject_id
                AND e.event_time BETWEEN a.admittime AND a.dischtime
        ) TO '{echo_meas_hadm_tmp}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """)
    n = conn.execute(f"SELECT COUNT(*) FROM '{echo_meas_hadm_tmp}'").fetchone()[0]
    n_hadm = conn.execute(f"SELECT COUNT(hadm_id) FROM '{echo_meas_hadm_tmp}'").fetchone()[0]
    print(f"  echo_meas: {n:,} rows, {n_hadm:,} with hadm_id ({100*n_hadm/max(n,1):.1f}%)")

    # Note events already have hadm_id from source -> alias for location join
    for tbl in ["rad_note_events", "dc_note_events"]:
        conn.execute(f"ALTER TABLE {tbl} RENAME TO {tbl}_hadm")

    # ──────────────────────────────────────────────────
    # Step 5: Location join using ASOF JOIN
    # ──────────────────────────────────────────────────
    print("Step 5: Preparing ASOF join for location...")
    conn.execute("""
        CREATE OR REPLACE TABLE location_sorted AS
        SELECT * FROM location_lookup
        ORDER BY subject_id, hadm_id, intime
    """)

    # Small tables: imaging events & note events
    print("  Joining location for imaging & note events...")
    for tbl in ["cxr_events", "echo_events", "ecg_events",
                 "rad_note_events", "dc_note_events"]:
        conn.execute(f"""
            CREATE OR REPLACE TABLE {tbl}_final AS
            SELECT
                e.subject_id, e.hadm_id, e.event_time, e.modality,
                COALESCE(t.location_type, 'Unknown') AS location_type,
                t.careunit,
                e.item_ids, e.item_labels, e."values",
                e.file_path, e.study_id
            FROM {tbl}_hadm e
            ASOF LEFT JOIN location_sorted t
                ON e.subject_id = t.subject_id
                AND e.hadm_id = t.hadm_id
                AND e.event_time >= t.intime
        """)

    # Echo measurements location join -> parquet
    echo_meas_final_tmp = str(TMP_DIR / "echo_meas_final.parquet")
    print("  Joining location for echo measurements -> parquet...")
    conn.execute(f"""
        COPY (
            SELECT
                e.subject_id, e.hadm_id, e.event_time, e.modality,
                COALESCE(t.location_type, 'Unknown') AS location_type,
                t.careunit,
                e.item_ids, e.item_labels, e."values",
                e.file_path, e.study_id
            FROM '{echo_meas_hadm_tmp}' e
            ASOF LEFT JOIN location_sorted t
                ON e.subject_id = t.subject_id
                AND e.hadm_id = t.hadm_id
                AND e.event_time >= t.intime
        ) TO '{echo_meas_final_tmp}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """)

    # Lab events: ASOF join -> parquet (large)
    print("Step 6a: Lab + location (ASOF join) -> parquet...")
    lab_final_tmp = str(TMP_DIR / "lab_final.parquet")
    conn.execute(f"""
        COPY (
            SELECT
                e.subject_id, e.hadm_id, e.event_time, e.modality,
                COALESCE(t.location_type, 'Unknown') AS location_type,
                t.careunit,
                e.item_ids, e.item_labels, e."values",
                e.file_path, e.study_id
            FROM '{lab_grouped_tmp}' e
            ASOF LEFT JOIN location_sorted t
                ON e.subject_id = t.subject_id
                AND e.hadm_id = t.hadm_id
                AND e.event_time >= t.intime
        ) TO '{lab_final_tmp}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """)
    n_lab = conn.execute(f"SELECT COUNT(*) FROM '{lab_final_tmp}'").fetchone()[0]
    print(f"  -> {n_lab:,} lab events exported")

    # Vital events: ASOF join -> parquet (large)
    print("Step 6b: Vital + location (ASOF join) -> parquet...")
    vital_final_tmp = str(TMP_DIR / "vital_final.parquet")
    conn.execute(f"""
        COPY (
            SELECT
                e.subject_id, e.hadm_id, e.event_time, e.modality,
                COALESCE(t.location_type, 'Unknown') AS location_type,
                t.careunit,
                e.item_ids, e.item_labels, e."values",
                e.file_path, e.study_id
            FROM '{vital_grouped_tmp}' e
            ASOF LEFT JOIN location_sorted t
                ON e.subject_id = t.subject_id
                AND e.hadm_id = t.hadm_id
                AND e.event_time >= t.intime
        ) TO '{vital_final_tmp}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """)
    n_vital = conn.execute(f"SELECT COUNT(*) FROM '{vital_final_tmp}'").fetchone()[0]
    print(f"  -> {n_vital:,} vital events exported")

    # ──────────────────────────────────────────────────
    # Step 7: Combine and export
    # ──────────────────────────────────────────────────
    print("Step 7: Exporting unified table (all modalities)...")
    conn.execute(f"""
        COPY (
            SELECT * FROM '{lab_final_tmp}'
            UNION ALL SELECT * FROM '{vital_final_tmp}'
            UNION ALL SELECT * FROM cxr_events_final
            UNION ALL SELECT * FROM echo_events_final
            UNION ALL SELECT * FROM ecg_events_final
            UNION ALL SELECT * FROM '{echo_meas_final_tmp}'
            UNION ALL SELECT * FROM rad_note_events_final
            UNION ALL SELECT * FROM dc_note_events_final
        ) TO '{OUTPUT_ALL}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """)

    n_all = conn.execute(f"SELECT COUNT(*) FROM '{OUTPUT_ALL}'").fetchone()[0]
    print(f"  -> {n_all:,} total unified events")

    # ──────────────────────────────────────────────────
    # Validation
    # ──────────────────────────────────────────────────
    print("\n=== Validation ===")
    r = conn.execute(f"""
        SELECT modality, COUNT(*) AS n,
               COUNT(DISTINCT subject_id) AS n_subjects,
               ROUND(100.0 * COUNT(hadm_id) / COUNT(*), 1) AS hadm_fill_pct,
               ROUND(100.0 * SUM(CASE WHEN location_type != 'Unknown' THEN 1 ELSE 0 END) / COUNT(*), 1) AS location_fill_pct
        FROM '{OUTPUT_ALL}'
        GROUP BY modality ORDER BY modality
    """).fetchdf()
    print(r.to_string(index=False))

    print("\nLocation distribution:")
    r = conn.execute(f"""
        SELECT location_type, COUNT(*) AS n,
               ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct
        FROM '{OUTPUT_ALL}'
        GROUP BY location_type ORDER BY COUNT(*) DESC
    """).fetchdf()
    print(r.to_string(index=False))

    print("\nSample lab event (LIST columns):")
    r = conn.execute(f"""
        SELECT subject_id, event_time, item_ids, item_labels, "values"
        FROM '{OUTPUT_ALL}'
        WHERE modality = 'lab' AND len(item_ids) > 3
        LIMIT 3
    """).fetchdf()
    print(r.to_string(index=False))

    # Clean up
    conn.close()
    shutil.rmtree(TMP_DIR, ignore_errors=True)

    print(f"\nDone! Output: {OUTPUT_ALL}")


if __name__ == "__main__":
    main()
