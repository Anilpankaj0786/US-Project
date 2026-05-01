# ✈️ US Domestic Flight Delay Analytics Platform

> A full end-to-end Data Engineering project built on **Amazon S3 · Databricks · Apache Spark · Delta Lake · Snowflake**

---

## 📌 Project Overview

This project builds a production-grade **Medallion Architecture Data Lakehouse** for analyzing US domestic flight delays from 2021–2023. The data comes from the Bureau of Transportation Statistics (BTS TranStats) — the official US government record of every domestic commercial flight.

The idea behind this project was simple: raw flight data was sitting in disconnected CSV files with no central storage, no versioning, and no automated pipeline. Analysts were spending 3–4 days every month just cleaning the data before any reporting could begin. This project fixes all of that.

---

## 🧱 Architecture — Medallion Lakehouse

```
BTS TranStats (Source)
        │
        ▼
  [ Phase 0 ]  Python Script → Downloads ZIPs → Extracts CSVs → Uploads to S3
        │
        ▼
  Amazon S3  (Raw Landing Zone)
        │
        ▼
  [ Phase 1 ]  Databricks + Spark
  ┌─────────────────────────────────┐
  │         BRONZE LAYER            │  ← Raw ingestion + DQ flags
  │   Delta Lake (partitioned by    │
  │       year / month)             │
  └─────────────────────────────────┘
        │
        ▼
  [ Phase 2 ]  Databricks + Spark
  ┌─────────────────────────────────┐
  │         SILVER LAYER            │  ← Cleaned, deduplicated, enriched
  │   Delta Lake (ZORDER optimized) │
  └─────────────────────────────────┘
        │
        ▼
  [ Phase 3 ]  Databricks + Spark
  ┌─────────────────────────────────┐
  │          GOLD LAYER             │  ← Pre-aggregated KPI tables
  │   Delta Lake + Snowflake        │
  └─────────────────────────────────┘
        │
        ▼
  [ Phase 4 ]  Snowflake + Power BI
  ┌─────────────────────────────────┐
  │       REPORTING LAYER           │  ← Views, Dashboards
  │   Snowflake REPORTING schema    │
  └─────────────────────────────────┘
```

---

## 🛠️ Tech Stack

| Tool | Purpose | Tier Used |
|------|---------|-----------|
| Python | Data download, S3 upload | Free |
| Amazon S3 | Raw landing + Delta storage | Free tier (~5 GB free) |
| Databricks | Managed Spark environment | Community Edition (Free) |
| Apache Spark | Distributed data processing | Included in Databricks |
| Delta Lake | ACID table format (Bronze/Silver/Gold) | Included in Databricks |
| Snowflake | Serving layer + SQL analytics | 30-day free trial ($400 credit) |
| Power BI Desktop | Dashboard & visualization | Free forever |

**Estimated Total Cost: $5–$20** (well within free/trial limits)

---

## 📂 Project Files

```
├── code/
│   ├── download_files1.py               # Phase 0 — BTS data download + S3 upload
│
├── notebooks/
│   ├── Airline_project_day2.ipynb       # Phase 1 — Bronze Layer (Schema + DQ flags)
│   ├── Airline-Project-day3.ipynb       # Phase 2 — Silver Layer (Transformations)
│   ├── Airline_project_day4.ipynb       # Phase 2/3 — Silver enrichment + Gold KPIs
│   ├── Snowflake_Spark_Connector_Day5.ipynb  # Phase 3/4 — Snowflake load
│
└── README.md
```

---

## 📊 Dataset Details

- **Source:** Bureau of Transportation Statistics (BTS) TranStats
- **Coverage:** 36 months — January 2021 to December 2023
- **Size:** ~7 GB uncompressed CSV
- **Rows:** ~18–20 million flights
- **Format:** One ZIP file per month (~120–180 MB compressed)

> ⚠️ **DE Note:** BTS CSVs end every row with a trailing comma — always use an explicit schema, never `inferSchema=True` on production BTS files.

---

## 🚀 Phase-by-Phase Breakdown

---

### Phase 0 — Environment Setup & Data Download

**File:** `download_files1.py`

This script handles everything before the actual Spark processing begins. It connects to the BTS website, downloads monthly ZIP files for the years configured, extracts the CSVs locally, and uploads them to S3 in a clean partitioned structure.

**What it does:**
- Downloads monthly ZIP files from `transtats.bts.gov/PREZIP/`
- Extracts CSVs to a local `./data/processed/` folder
- Uploads to S3 as `s3://airlines-bucket-07860/download/year=YYYY/month=M/filename.csv`
- Cleans up local files only after a confirmed successful upload

**S3 folder structure after Phase 0:**
```
s3://airlines-bucket-07860/
├── download/
│   ├── year=2021/month=1/  ← On_Time_Reporting_Carrier...csv
│   ├── year=2021/month=2/
│   ...
├── raw/airports/           ← OpenFlights airport reference
└── raw/carriers/           ← BTS carrier reference
```

**Key code snippet:**
```python
# Partitioned upload to S3
s3_key = f"download/year={year}/month={month}/{file}"
upload_file_to_s3(full_path, BUCKET_NAME, s3_key)

# Cleanup only after successful upload
if upload_success:
    cleanup_local(zip_path, extract_path)
```

---

### Phase 1 — Bronze Layer

**File:** `Airline_project_day2.ipynb` | *Estimated runtime: 25–40 min*

This is where raw CSV data hits Spark for the first time. The goal here is not to transform anything — it's purely to ingest reliably and attach data quality flags so downstream layers know what they're working with.

**Schema — 35 explicit columns defined:**
```python
schema = StructType([
    StructField("FlightDate", DateType(), True),
    StructField("Reporting_Airline", StringType(), True),
    StructField("Origin", StringType(), True),
    StructField("Dest", StringType(), True),
    StructField("DepDelay", DoubleType(), True),
    StructField("ArrDelay", DoubleType(), True),
    StructField("Cancelled", DoubleType(), True),
    # ... 28 more columns
])
```

**Reading with corrupt record handling:**
```python
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("mode", "permissive") \
    .option("columnNameOfCorruptRecord", "_corrupt_record") \
    .schema(schema) \
    .load("s3a://airlines-bucket-07860/download/*/*/")
```

**DQ Flag Columns added at Bronze (observe-only, no rows removed):**

| Flag Column | Condition | Expected Rate |
|-------------|-----------|---------------|
| `flight_date_null` | FlightDate IS NULL | < 0.001% |
| `reporting_airline_null` | Airline IS NULL | < 0.001% |
| `origin_null` / `destination_null` | Either IS NULL | < 0.001% |
| `departure_delay_range_flag` | DepDelay NOT BETWEEN -120 and 1440 | < 0.1% |
| `check_distance_range` | Distance NOT BETWEEN 1 and 10000 | < 0.01% |
| `same_origin_destination` | Origin == Dest | < 0.001% |

**Using `withColumn` vs `withColumns` — Both approaches demonstrated:**
```python
# Approach 1: withColumn() — one at a time
df = df.withColumn("flight_date_null",
    f.when(f.col("FlightDate").isNull(), 1).otherwise(0))

# Approach 2: withColumns() — dictionary-based, all at once
null_column_dict = {
    "flight_date_null": f.when(f.col("FlightDate").isNull(), 1).otherwise(0),
    "origin_null":      f.when(f.col("Origin").isNull(), 1).otherwise(0),
    # ...
}
df = df.withColumns(null_column_dict)
```

**Writing to Bronze Delta (partitioned):**
```python
final_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .partitionBy("year", "month") \
    .save("s3a://airlines-bucket-07860/Bronze_data/flight")
```

> 🔑 **Golden Rule at Bronze:** *Observe. Never discard. Flag it, count it, and let downstream layers decide.*

---

### Phase 2 — Silver Layer

**Files:** `Airline-Project-day3.ipynb` + `Airline_project_day4.ipynb` | *Estimated runtime: 15–25 min*

Silver is where the actual transformation work happens. Every incoming flight record is cleaned, validated, enriched with airport data, and deduplicated.

#### 2.1 — Airport Delta Table (JSON → Delta)

```python
# Read airport JSON from S3, filter US airports only
df_airport = spark.read \
    .option("multiLine", "true") \
    .schema(airlines_schema) \
    .json("s3://airlines-bucket-07860/Airport_data/")

df_us = df_airport.filter(col("country") == "US")
df_us.write.format("delta").mode("overwrite") \
    .save("s3://airlines-bucket-07860/Airport_data/delta/")
```

#### 2.2 — HHMM → HH:MM Time Conversion

BTS stores scheduled times as integers like `1430` (meaning 2:30 PM). Two versions were built — v1 handles the special midnight case (2400), v2 adds null safety:

```python
# Version 2 (null-safe)
df = df.withColumn("crs_dep_minutes_safe",
    when(col("CRSDepTime").isNull(), None)
    .when(col("CRSDepTime") == 2400, 0)
    .otherwise(floor(col("CRSDepTime") / 100) * 60 + (col("CRSDepTime") % 100))
)
```

#### 2.3 — Delay Category Bucketing

```python
df = df.withColumn("arrival_delay_category_bucket",
    when(col("Cancelled") == 1,                                       "Cancelled")
    .when(col("ArrDelayMinutes") < 0,                                 "Early")
    .when((col("ArrDelayMinutes") >= 0)  & (col("ArrDelayMinutes") <= 14),  "On Time")
    .when((col("ArrDelayMinutes") >= 15) & (col("ArrDelayMinutes") <= 44),  "Minor Delay")
    .when((col("ArrDelayMinutes") >= 45) & (col("ArrDelayMinutes") <= 120), "Major Delay")
    .when(col("ArrDelayMinutes") > 120,                               "Severe Delay")
)
```

#### 2.4 — Boolean Columns

```python
df = df \
    .withColumn("is_cancelled",         when(col("Cancelled") == 1, True).otherwise(False)) \
    .withColumn("is_diverted",          when(col("Diverted") == 1, True).otherwise(False)) \
    .withColumn("is_departure_delayed", when(col("DepDel15") == 1, True).otherwise(False)) \
    .withColumn("is_arrival_delayed",   when(col("ArrivalDelayGroups") > 0, True).otherwise(False))
```

#### 2.5 — Deduplication with Window Functions

BTS occasionally re-files flight records. Window functions are used to keep only the latest version of each flight:

```python
dedup_window = Window.partitionBy(
    "FlightDate", "Reporting_Airline",
    "Flight_Number_Reporting_Airline", "Origin", "Dest"
).orderBy(col("load_timestamp").desc())

df = df.withColumn("row_num", row_number().over(dedup_window))
df = df.filter(col("row_num") == 1).drop("row_num")
```

#### 2.6 — Airport Broadcast Join

The small airport DataFrame (~900 US rows) is broadcast to all executors, avoiding a full shuffle of the 7 GB flight data:

```python
# Origin airport join
df_silver = df.join(
    broadcast(df_origin_airport),
    df.origin_code == df_origin_airport.origin_iata,
    how="left"
).drop("origin_iata")

# Destination airport join
df_silver = df_silver.join(
    broadcast(df_dest_airport),
    df_silver.destination_code == df_dest_airport.dest_iata,
    how="left"
).drop("dest_iata")
```

#### 2.7 — Custom UDF: Flight Number Character Extraction

```python
def extract_flight_chars(flight_num):
    """Extract 1st, 4th, and 7th characters from flight number."""
    if flight_num is None:
        return None
    s = str(flight_num)
    return "".join(s[idx] for idx in [0, 3, 6] if idx < len(s)) or None

extract_flight_chars_udf = udf(extract_flight_chars, StringType())
df_silver = df_silver.withColumn("flight_num_extract",
    extract_flight_chars_udf(col("Flight_Number_Reporting_Airline")))
```

#### 2.8 — Haversine Distance (Native Spark — No UDF)

Great-circle distance computed using native Spark functions for maximum distributed performance:

```python
from pyspark.sql.functions import radians, sin, cos, sqrt, atan2, lit

# R = 6371 km (Earth radius)
# Uses lat/lon enriched by airport broadcast join
```

**Saving Silver Delta:**
```python
df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .option("delta.dataSkippingNumIndexedCols", "-1") \
    .partitionBy("year", "month") \
    .save("s3://airlines-bucket-07860/silver_data/flight")
```

---

### Phase 3 — Gold Layer

**File:** `Airline_project_day4.ipynb` | *Estimated runtime: 10–15 min*

Gold layer computes four business-ready KPI tables. The Silver DataFrame is cached before building all four aggregations to avoid re-reading from S3 multiple times.

#### Gold Table 1 — `gold_airline_kpi` (Monthly Airline KPIs)

```python
GROUP_COLS = ["flight_year", "flight_month", "airline_code", "Reporting_Airline"]

# Base flight counts
df_base = df_silver.groupBy(*GROUP_COLS).agg(
    count("*").alias("total_number_of_flights"),
    spark_sum(col("is_arrival_delayed").cast("int")).alias("delayed_flights"),
    spark_sum(col("is_cancelled").cast("int")).alias("total_flights_cancelled"),
    spark_sum(col("is_diverted").cast("int")).alias("diverted_flights")
)

# Average + Median delay (non-cancelled only)
df_delay_stats = df_silver.filter(col("is_cancelled") == False) \
    .groupBy(*GROUP_COLS).agg(
        avg(col("ArrDelayMinutes")).alias("avg_arrival_delay_minutes"),
        percentile_approx(col("ArrDelayMinutes"), 0.5).alias("median_arrival_delay_minutes")
    )

# Monthly rank by total flights
rank_window = Window.partitionBy("flight_year", "flight_month") \
                    .orderBy(col("total_number_of_flights").desc())
gold_airline_kpi = gold_airline_kpi.withColumn("monthly_rank", dense_rank().over(rank_window))
```

#### Gold Table 2 — `gold_route_kpi` (Annual Route Performance)

```python
df_route = df_silver.withColumn("route",
    concat_ws("-", col("origin_code"), col("destination_code")))

gold_route_kpi = df_route.groupBy("flight_year", "route", "origin_code", "destination_code").agg(
    count("*").alias("number_of_flights"),
    round(avg(when(col("is_cancelled") == False, col("ArrDelayMinutes"))), 2).alias("avg_arrival_delay"),
    round(avg("Distance"), 2).alias("avg_distance_travelled"),
    sum(when(col("is_arrival_delayed") == True, 1).otherwise(0)).alias("total_delayed_flights")
)
```

#### Gold Table 3 — `gold_airport_departure_kpi` (Airport Departure Metrics)

Groups by origin airport with city, state, lat/lon for Power BI map visualization. Calculates total departures, cancellations, average departure delay, and on-time departure percentage.

#### Gold Table 4 — `df_delay_cause_table` (Delay Cause Analysis)

```python
df_delay_cause_table = df_silver \
    .filter((col("is_cancelled") == False) & (col("ArrDelayMinutes") > 0)) \
    .groupBy("flight_year", "flight_month", "airline_code").agg(
        round(sum("ArrDelayMinutes"), 2).alias("total_minutes_delayed"),
        round(sum("WeatherDelay"),    2).alias("total_weather_delayed_minutes"),
        round(sum("CarrierDelay"),    2).alias("total_carrier_delayed_minutes"),
        round(sum("NASDelay"),        2).alias("total_nas_delayed_minutes"),
        round(sum("LateAircraftDelay"), 2).alias("total_late_aircraft_delayed_minutes")
    )
```

---

### Phase 4 — Snowflake Load & Reporting

**File:** `Snowflake_Spark_Connector_Day5.ipynb`

Gold tables are read from Databricks Delta and written to S3 as Parquet first, then loaded into Snowflake using the Spark-Snowflake connector.

```python
# Snowflake connection options
options = {
    "sfURL":       "XSFBMIH-AN13631.snowflakecomputing.com",
    "sfDatabase":  "FLIGHT_ANALYSIS",
    "sfSchema":    "STAGING",
    "sfWarehouse": "COMPUTE_WH",
    "sfRole":      "ACCOUNTADMIN"
}

# Write Gold tables to S3 Parquet
gold_airline_kpi.write.mode("overwrite") \
    .parquet("s3://airlines-bucket-07860/Gold_data/monthly_airline_kpi/")

# Read back from Snowflake (for validation)
df = spark.read.format("snowflake") \
    .options(**options) \
    .option("query", "SELECT * FROM delay_cause_table") \
    .load()
```

**Snowflake Reporting Views created for Power BI:**
- `vw_airline_scorecard` — annual performance ranking per airline
- `vw_delay_trend_monthly` — month-by-month trend with season/quarter context
- `vw_airport_performance_map` — airport lat/lon + performance metrics for map visual

---

## 📈 KPIs Delivered

| KPI | Definition | Source Columns |
|-----|-----------|----------------|
| On-Time Rate (%) | Flights arriving ≤15 min late / Total completed × 100 | `ArrDel15`, `Cancelled` |
| Avg Arrival Delay | Mean arrival delay in minutes (non-cancelled) | `ArrDelay` |
| Cancellation Rate | Cancelled flights / Total flights × 100 | `Cancelled` |
| Delay Cause Mix | % of total delay minutes by cause | `CarrierDelay`, `WeatherDelay`, `NASDelay`, etc. |
| P95 Arrival Delay | 95th percentile delay — worst case passenger experience | `ArrDelay` |
| Monthly Airline Rank | `DENSE_RANK()` over total flights per month | Computed |

---

## 🗂️ Expected Data Volumes

| Layer | Table | Rows | Size |
|-------|-------|------|------|
| Bronze | `bronze_flights` | ~19–20 million | ~4.5 GB |
| Silver | `silver_flights` | ~18.5–19.5 million | ~3.8 GB |
| Gold | `airline_monthly_kpis` | ~500–600 | < 1 MB |
| Gold | `route_annual_performance` | ~15,000–20,000 | < 5 MB |
| Gold | `airport_monthly_performance` | ~10,000–15,000 | < 5 MB |
| Gold | `delay_cause_analysis` | ~500–600 | < 1 MB |

---

## ⚠️ Common Issues & Fixes

**S3 Access Denied in Databricks**
- AWS credentials must be set as environment variables on the cluster — never hardcoded in notebook cells
- Check `s3:GetObject`, `s3:PutObject`, `s3:ListBucket` permissions on the IAM user

**Cluster Terminates Mid-Job (Community Edition)**
- 2-hour timeout cannot be extended
- Solution: Split Bronze load into yearly batches — Delta is idempotent, safe to re-run

**BTS CSV Corrupt Records**
- Every BTS row ends with a trailing comma (extra null column)
- The explicit 35-column schema handles this — never add a 36th/37th field

**Snowflake Connector ClassNotFoundException**
- Maven library must be installed on the cluster *before* starting the notebook
- Cluster restart required after adding libraries

---

## 📉 Approximate Results (2021–2023)

| Metric | Value | Note |
|--------|-------|------|
| Overall on-time rate | 75–80% | FAA target is 80% |
| Avg arrival delay | 12–16 minutes | Peaks in June–Aug and December |
| Avg cancellation rate | 2.5–4% | Higher in winter and storm months |
| Top delay cause | Late Aircraft (35–40%) | Cascading delays from previous flights |
| Best on-time airline | Alaska Airlines (~85%) | Consistent West Coast performer |
| Worst delay airport | EWR (Newark) | Chronic NAS congestion |

---

## 🏗️ Setup Instructions

### 1. AWS S3
```
1. Create bucket: airlines-bucket-07860 (or your preferred name)
2. Region: us-east-1
3. Block all public access: YES
4. Create IAM user with AmazonS3FullAccess
5. Save Access Key ID and Secret Access Key
```

### 2. Databricks Community Edition
```
1. Sign up: community.cloud.databricks.com (free, no credit card)
2. Runtime: 13.x LTS (Spark 3.4 + Delta Lake 2.4)
3. Add Maven libraries:
   - net.snowflake:spark-snowflake_2.12:2.12.0-spark_3.4
   - net.snowflake:snowflake-jdbc:3.14.3
4. Set env vars on cluster:
   AWS_ACCESS_KEY_ID=<your_key>
   AWS_SECRET_ACCESS_KEY=<your_secret>
```

### 3. Snowflake Free Trial
```
1. Sign up at snowflake.com (30-day trial, $400 credit)
2. Auto-suspend: 1 minute (IMPORTANT — prevents credit burn)
3. Warehouse: X-Small (1 credit/hour)
4. Create database: FLIGHT_ANALYSIS
```

### 4. Run Order
```
Phase 0 → download_files1.py         (local machine)
Phase 1 → Airline_project_day2.ipynb (Databricks)
Phase 2 → Airline-Project-day3.ipynb (Databricks)
Phase 2/3 → Airline_project_day4.ipynb (Databricks)
Phase 4 → Snowflake_Spark_Connector_Day5.ipynb (Databricks)
```

---

## 💡 Key Learnings

- **Medallion Architecture** Medallion Architecture organizes data into three layers—Bronze (raw, unprocessed data), Silver (cleaned and validated data), and Gold (trusted, business-ready data for analytics)—ensuring a scalable, reliable, and easy-to-maintain data pipeline.
- **Broadcast joins** are critical when joining a small reference table with a multi-GB fact table — skipping the broadcast hint would shuffle the entire 7 GB dataset unnecessarily
- **Window functions** for deduplication are more reliable than `dropDuplicates()` because you can control which version of a record to keep (latest by timestamp)
- **Native Spark functions > Python UDFs** wherever possible — the Haversine distance was implemented with native Spark to avoid serialization overhead
- **Delta Lake ZORDER** on frequently-filtered columns (`airline_code`, `origin_code`) reduced query I/O by 60–80% in testing
- **`withColumns()` is more efficient than multiple `withColumn()` calls** — it applies all column additions in a single pass over the DataFrame

---

## 👤 Author

**Anil Pankaj**
Project guided by [Regex Software Services](https://www.regexsoftware.com) — Jaipur, Rajasthan

---

*Built as a hands-on Data Engineering learning project covering the complete modern Data Lakehouse stack.*
