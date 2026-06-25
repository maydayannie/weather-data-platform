# Weather & Air Quality Data Platform

A production-grade, end-to-end data pipeline that ingests real-time weather and air quality data from the OpenWeatherMap API, transforms it through a multi-layer dbt model, and orchestrates daily execution via Dagster.

---

## Table of Contents

- [Architecture Overview](#architecture-overview)
- [Project Structure](#project-structure)
- [Data Sources](#data-sources)
- [Data Lineage](#data-lineage)
- [Core Business Logic](#core-business-logic)
- [Data Quality](#data-quality)
- [dbt Docs](#dbt-docs)
- [How to Run](#how-to-run)

---

## Architecture Overview

```
┌──────────────────────────────────────────────────────────────────────┐
│                        Dagster Orchestration                         │
│               Schedule: Daily 09:00 Asia/Taipei                      │
└──────────────────────┬───────────────────────────────────────────────┘
                       │
          ┌────────────▼───────────┐
          │  Asset 1               │
          │  raw_weather_and_      │
          │  pollution_api         │
          │  (fetch_weather.py)    │
          └────────────┬───────────┘
                       │
          ┌────────────▼───────────┐
          │  DuckDB (local)        │
          │  raw_weather_data      │
          │  raw_pollution_data    │
          └────────────┬───────────┘
                       │
          ┌────────────▼───────────┐
          │  Asset 2               │
          │  dbt_weather_models    │
          │  (dbt run)             │
          └────────────┬───────────┘
                       │
       ┌───────────────┼───────────────┐
       ▼               ▼               ▼
  stg_weather    stg_pollution   (downstream)
       │               │
       └───────┬───────┘
               ▼
  fct_regional_weather_alerts
               │
               ▼
  fct_global_weather_pollution
               │
               ▼
  scd_weather_snapshot (Type-2 SCD)
```

The pipeline has three layers:

| Layer | Tools | Responsibility |
|---|---|---|
| **Ingestion** | Python + OpenWeatherMap API | Fetch raw data and write to DuckDB |
| **Transformation** | dbt + DuckDB | Stage, clean, join, and enrich |
| **Orchestration** | Dagster | Schedule, dependency management, observability |

---

## Project Structure

```
project_dbt/
├── definitions.py              # Dagster assets, job, and schedule
├── fetch_weather.py            # API ingestion script
├── local_weather.duckdb        # Embedded DuckDB database
├── .env                        # API key (not committed)
└── dbt_weather/
    ├── dbt_project.yml
    ├── packages.yml
    ├── models/
    │   ├── staging/
    │   │   ├── schema.yml      # Column tests and source definitions
    │   │   ├── stg_weather.sql
    │   │   └── stg_pollution.sql
    │   └── marts/
    │       ├── fct_regional_weather_alerts.sql
    │       └── fct_global_weather_pollution.sql
    └── snapshots/
        └── scd_weather_snapshot.sql
```

---

## Data Sources

**API Provider**: [OpenWeatherMap](https://openweathermap.org/api)

### Monitored Cities

| Region | Cities |
|---|---|
| Taiwan | Taipei, Xizhi, Banqiao |
| Thailand | Bangkok, Pattaya |
| Japan | Tokyo |
| UK | London |

### API Endpoints

**Current Weather** — `GET /data/2.5/weather`

Returns: city, geographic coordinates, weather condition string, temperature (Kelvin), Unix timestamp.

**Air Pollution** — `GET /data/2.5/air_pollution`

Returns: AQI index (1–5), PM2.5 (µg/m³), PM10 (µg/m³), Unix timestamp. Coordinates from the weather response are passed as input.

---

## Data Lineage

```
OpenWeatherMap API
├── Weather endpoint  ──► raw_weather_data   (city, weather, temp_k, dt)
└── Pollution endpoint ──► raw_pollution_data (city, aqi, pm2_5, pm10, dt)
                                │
                    ┌───────────┴───────────┐
                    ▼                       ▼
              stg_weather             stg_pollution
         (incremental, Celsius,    (AQI labels, renamed
          deduplication)            columns)
                    │                       │
                    └───────────┬───────────┘
                                ▼
                  fct_regional_weather_alerts
               (temperature alerts, region tagging)
                                │
                                ▼
                  fct_global_weather_pollution
               (weather + pollution join, activity index)
                                │
                                ▼
                     scd_weather_snapshot
                  (Type-2 SCD, full alert history)
```

### Table Descriptions

| Table | Materialization | Description |
|---|---|---|
| `raw_weather_data` | Table (truncate-load) | Raw API weather response |
| `raw_pollution_data` | Table (truncate-load) | Raw API pollution response |
| `stg_weather` | Incremental | Cleaned weather: Kelvin→Celsius, deduped by (city, time) |
| `stg_pollution` | View | Cleaned pollution: AQI integer mapped to descriptive label |
| `fct_regional_weather_alerts` | Table | Temperature-based alerts with regional classification |
| `fct_global_weather_pollution` | Table | Joined weather + pollution with outdoor activity recommendation |
| `scd_weather_snapshot` | Snapshot (Type-2) | Full history of alert-level changes per city |

---

## Core Business Logic

### 1. Unit Conversion — `stg_weather`

Raw temperatures from the API are in Kelvin. The staging model converts to Celsius:

```sql
ROUND(temp_k - 273.15, 2) AS temperature_celsius
```

Incremental logic prevents reprocessing historical data:

```sql
WHERE dt > (SELECT MAX(observation_time) FROM {{ this }})
```

### 2. AQI Labeling — `stg_pollution`

The numeric AQI index (1–5) is mapped to a human-readable label:

| AQI | Label |
|---|---|
| 1 | 🟢 Good |
| 2 | 🟡 Fair |
| 3 | 🟠 Moderate |
| 4 | 🔴 Poor |
| 5 | 🟣 Very Poor |

### 3. Temperature Alert Levels — `fct_regional_weather_alerts`

Each observation receives a tiered heat alert:

| Condition | Alert Level |
|---|---|
| ≥ 35°C | 🔴 Extreme Heat |
| 28°C – 34.9°C | 🟠 Hot |
| 18°C – 27.9°C | 🟢 Comfortable |
| < 18°C | 🔵 Cool/Cold |

Cities are also tagged by region: Taipei, Xizhi, and Banqiao are classified as `Taiwan`; all others as `International`.

### 4. Outdoor Activity Index — `fct_global_weather_pollution`

The final fact table joins weather alerts with pollution data to generate an actionable recommendation:

| Condition | Recommendation |
|---|---|
| Alert = Comfortable **AND** AQI ≤ 2 | Perfect for Outdoor Sports! |
| AQI ≥ 4 | Warning: Stay Indoors! |
| All other cases | Acceptable for Casual Walk! |

### 5. Historical Tracking — `scd_weather_snapshot`

The snapshot uses dbt's built-in Type-2 SCD strategy, tracking changes to `alert_level` and `temperature_celsius` per city over time. Each row is annotated with `dbt_valid_from` and `dbt_valid_to`, enabling full historical queries and trend analysis.

---

## Data Quality

Tests are defined in `dbt_weather/models/staging/schema.yml` and run automatically after each transformation.

| Model | Column | Test |
|---|---|---|
| `stg_weather` | `city` | `not_null` |
| `stg_weather` | `observation_time` | `not_null` |
| `stg_weather` | `temperature_celsius` | `not_null` |
| `stg_weather` | `temperature_celsius` | `accepted_range` (−30°C to 32°C) |

---

## dbt Docs

dbt generates a static documentation site that includes model descriptions, column definitions, data lineage DAG, and test coverage. All descriptions are sourced from `dbt_weather/models/staging/schema.yml`.

### Generate docs

```bash
cd dbt_weather
dbt docs generate
```

This compiles the catalog and manifest into the `target/` directory.

### Serve docs locally

```bash
dbt docs serve
```

Opens the docs site at `http://localhost:8080`. The site includes:

- **Lineage graph** — interactive DAG from raw sources through staging to marts and snapshot
<img width="1338" height="730" alt="image" src="https://github.com/user-attachments/assets/bf71e2a6-12f0-4103-8087-e6247fdbca81" />

- **Model pages** — materialization strategy, description, column-level metadata, and tests for each model
- **Source freshness** — tracks when `raw_weather_data` and `raw_pollution_data` were last loaded
- **Test results** — which `not_null` and `accepted_range` assertions are attached to each column
<img width="1360" height="761" alt="image" src="https://github.com/user-attachments/assets/cf63cf36-93df-43a2-aa52-eb90ee210b7a" />


### Documented models

| Model | Description |
|---|---|
| `stg_weather` | Staging table for weather monitoring data with incremental cleaning and unit conversion |
| `stg_pollution` | AQI label mapping from raw numeric index |
| `fct_regional_weather_alerts` | Temperature alerts with Taiwan / International region tagging |
| `fct_global_weather_pollution` | Joined weather + pollution with outdoor activity recommendation |
| `scd_weather_snapshot` | Type-2 SCD tracking alert-level and temperature changes per city |

---

## How to Run

### Prerequisites

- Python 3.9+
- [uv](https://github.com/astral-sh/uv) or pip
- dbt-duckdb
- Dagster

### 1. Install Dependencies

```bash
pip install dagster dagster-dbt dbt-duckdb python-dotenv requests
```

### 2. Configure API Key

Create a `.env` file in the project root:

```bash
OPENWEATHER_API_KEY=your_openweathermap_api_key_here
```

### 3. Install dbt Packages

```bash
cd dbt_weather
dbt deps
```

### 4. Run the Full Pipeline Manually

**Step 1 — Fetch data from API:**

```bash
python fetch_weather.py
```

**Step 2 — Run dbt transformations:**

```bash
cd dbt_weather
dbt run
```

**Step 3 — Run dbt snapshot:**

```bash
dbt snapshot
```

**Step 4 — Run data quality tests:**

```bash
dbt test
```

### 5. Launch Dagster UI (Recommended)

Dagster manages the full pipeline including the daily schedule:

```bash
dagster dev -f definitions.py
```

Open `http://localhost:3000` in your browser. From the Dagster UI you can:

- Materialize all assets on demand
- Enable the `weather_daily_9am_schedule` for automated daily runs
<img width="822" height="341" alt="image" src="https://github.com/user-attachments/assets/be1b5dd2-3dba-4b9f-aab3-33c4255f483d" />

- Inspect asset lineage, run logs, and partition history
<img width="1461" height="775" alt="image" src="https://github.com/user-attachments/assets/95b73b20-43f0-41d6-9dfa-45f38a6171a7" />


### 6. Verify Output

Connect to DuckDB to inspect the final tables:

```bash
# Open the database using the DuckDB CLI
duckdb local_weather.duckdb

# Run the query to check the final integrated weather and pollution table
SELECT * FROM main.fct_global_weather_pollution;
```
<img width="915" height="326" alt="image" src="https://github.com/user-attachments/assets/bdce2f72-0f4e-4fd0-aa3f-6b89bb756313" />
