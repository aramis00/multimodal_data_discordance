# Multimodal Data Discordance

Unified multimodal event table for MIMIC-IV, integrating structured data, imaging, and clinical notes into a single time-indexed long-format table.

## Overview

This project consolidates heterogeneous clinical data sources from the MIMIC-IV ecosystem into a single Parquet file (`unified_event_table.parquet`). Each row represents a clinical event with a timestamp, patient/admission identifiers, care location, and modality-specific payloads.

The primary use case is analyzing temporal discordance across multimodal clinical data in hospitalized patients with heart failure.

## Modalities (8 types)

| Category | Modality | Source | Description |
|----------|----------|--------|-------------|
| Structured | `lab` | hosp/labevents | 50 major blood lab items (Blood Gas, Chemistry, Hematology) |
| Structured | `vital` | icu/chartevents | 11 vital sign items (HR, BP, RR, Temp, SpO2, CVP) |
| Structured | `echo_measurement` | mimic-iv-echo-note | Echo machine values (LVEF, E/A ratio, wall thickness, etc.) |
| Imaging | `cxr` | mimic-cxr-jpg | Chest X-ray studies |
| Imaging | `ecg` | mimic-iv-ecg | 12-lead ECG records |
| Imaging | `echo` | mimic-iv-echo | Echocardiogram DICOM studies |
| Notes | `radiology_note` | mimic-iv-note | Radiology reports (event marker; text via note_id) |
| Notes | `discharge_note` | mimic-iv-note | Discharge summaries (event marker; text via note_id) |

Structured modalities are aggregated at hourly resolution (median per item, then LIST-aggregated per time point). Imaging and note modalities are stored as event markers with `file_path`/`study_id` for linking back to source data.

## Schema

| Column | Type | Description |
|--------|------|-------------|
| `subject_id` | BIGINT | Patient ID |
| `hadm_id` | BIGINT | Hospital admission ID (NULL if unmatched) |
| `event_time` | TIMESTAMP | Event timestamp (hourly-truncated for structured) |
| `modality` | VARCHAR | One of the 8 modality types |
| `location_type` | VARCHAR | ER / ICU / Ward / Unknown |
| `careunit` | VARCHAR | Specific care unit name from transfers |
| `item_ids` | BIGINT[] | Item IDs (lab/vital only) |
| `item_labels` | VARCHAR[] | Item names or note_type |
| `values` | DOUBLE[] | Numeric values (lab/vital/echo_measurement only) |
| `file_path` | VARCHAR | Relative path to source file (imaging only) |
| `study_id` | BIGINT | Study or note ID |

## Project Structure

```
multimodal_data_discordance/
├── README.md
├── src/
│   └── create_unified_event_table.py   # Main ETL script
├── data/
│   └── unified_event_table.parquet     # Output (gitignored)
└── doc/
    ├── mimic_data_mapping.xlsx         # Data mapping reference
    └── multimodal_discordance_v4_1.pptx
```

## Data Sources

All source data must be downloaded from [PhysioNet](https://physionet.org/) with appropriate credentialed access.

| Dataset | Version | PhysioNet URL |
|---------|---------|---------------|
| MIMIC-IV | 3.1 | https://physionet.org/content/mimiciv/ |
| MIMIC-CXR-JPG | 2.1 | https://physionet.org/content/mimic-cxr-jpg/ |
| MIMIC-IV-ECG | 1.0 | https://physionet.org/content/mimic-iv-ecg-diagnostic-electrocardiogram-matched-subset/ |
| MIMIC-IV-Echo | 0.1 | https://physionet.org/content/mimic-iv-echo/ |
| MIMIC-IV-Echo-Note | - | https://physionet.org/content/mimic-iv-echo-note/ |
| MIMIC-IV-Note | 2.2 | https://physionet.org/content/mimic-iv-note/ |

## Requirements

- Python 3.9+
- [DuckDB](https://duckdb.org/) (`pip install duckdb`)

## Usage

1. Update the `PHYSIONET` path in `src/create_unified_event_table.py` to point to your local PhysioNet data directory.

2. Run the ETL script:

```bash
python src/create_unified_event_table.py
```

The script processes all modalities in sequence, performing hourly aggregation, hadm_id matching, and location assignment via ASOF JOIN from the transfers table. Output is written to `data/unified_event_table.parquet`.

## Processing Pipeline

1. **Location lookup** — Build ER/ICU/Ward classification from `hosp/transfers`
2. **Admissions** — Load admission windows for hadm_id matching
3. **Modality extraction** (Steps 3a–3h) — Extract and aggregate each modality
4. **hadm_id fill** — Assign hospital admission IDs to imaging/echo measurements via time-window matching
5. **Location join** — ASOF JOIN to assign care location at event time
6. **Export** — UNION ALL and write to Parquet (ZSTD compressed)

## License

This project uses MIMIC-IV data, which requires a signed data use agreement with PhysioNet. Do not redistribute the source data or derived datasets containing protected health information.
