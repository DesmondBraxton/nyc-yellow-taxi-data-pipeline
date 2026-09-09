# nyc-yellow-taxi-data-pipeline
Data engineering pipeline for NYC Yellow Taxi data using PySpark, AWS S3, Athena, Redshift, and SQL.

# NYC Yellow Taxi Data Engineering Pipeline

An end-to-end data engineering project that processes NYC TLC Yellow Taxi trip data using PySpark and an AWS-oriented architecture.

The pipeline ingests monthly Parquet datasets, validates and transforms trip records, quarantines records that fail analytical quality rules, enriches trips with NYC taxi-zone information, and produces partitioned curated datasets for downstream analytics with Amazon Athena and Amazon Redshift.

The project was built to demonstrate a production-style data engineering workflow while keeping the code modular, testable, and scalable.

---

## Architecture

![NYC Yellow Taxi Data Pipeline Architecture](architecture/pipeline_architecture.jpg)

### Pipeline Flow

```text
NYC TLC Yellow Taxi Data
        |
        v
Amazon S3 - Raw Layer
        |
        v
PySpark ETL / AWS Glue
        |
        +-------------------+
        |                   |
        v                   v
Invalid Records        Valid Records
        |                   |
        v                   v
S3 Quarantine      Transform + Enrich
                            |
                            v
                     S3 Curated Layer
                     Parquet + Partitions
                            |
                            v
                    AWS Glue Data Catalog
                            |
                            v
                       Amazon Athena
                            |
                            v
                      Amazon Redshift
                            |
                            v
                  Analytics / Dashboard
```

---

## Project Goals

The goal of this project is to build a data pipeline that demonstrates more than simply loading a dataset and running SQL.

The pipeline was designed around several common data engineering requirements:

- Process multiple monthly source files
- Separate ingestion from transformation logic
- Validate incoming records
- Quarantine records that fail quality rules
- Create reusable analytical features
- Enrich fact data using a reference dataset
- Store curated data in an analytics-friendly format
- Partition large datasets for efficient querying
- Support serverless SQL analysis with Athena
- Model curated data for a Redshift warehouse
- Test important transformation logic
- Keep the PySpark code portable between local development and AWS Glue

---

## Data Source

The project uses NYC Taxi & Limousine Commission Yellow Taxi Trip Record data.

The initial pipeline is designed around:

```text
yellow_tripdata_2025-01.parquet
yellow_tripdata_2025-02.parquet
yellow_tripdata_2025-03.parquet
```

The source data contains fields such as:

- pickup and dropoff timestamps
- pickup and dropoff location IDs
- passenger count
- trip distance
- payment type
- fare amount
- tip amount
- tolls
- total trip amount

A Taxi Zone Lookup dataset is also used to translate location IDs into human-readable borough and taxi-zone information.

Large raw datasets are intentionally excluded from the GitHub repository.

---

## Technology Stack

| Technology | Purpose |
|---|---|
| Python | Pipeline development |
| PySpark | Distributed data processing |
| Apache Parquet | Columnar storage |
| Amazon S3 | Raw, curated, and quarantine storage |
| AWS Glue | AWS PySpark ETL execution target |
| AWS Glue Data Catalog | Table metadata and schema management |
| Amazon Athena | Serverless SQL analytics over S3 |
| Amazon Redshift | Analytical data warehouse |
| SQL | Transformation and analytical querying |
| pytest | Automated pipeline testing |
| Git / GitHub | Source control and project documentation |

---

## Repository Structure

```text
nyc-yellow-taxi-data-pipeline/
│
├── architecture/
│   └── pipeline_architecture.jpg
│
├── src/
│   ├── ingestion.py
│   ├── data_quality.py
│   ├── transformations.py
│   ├── enrichment.py
│   └── pipeline.py
│
├── sql/
│   ├── athena_queries.sql
│   └── redshift_queries.sql
│
├── tests/
│   └── test_transformations.py
│
├── data/
│   ├── raw/
│   │   └── yellow_taxi/
│   ├── reference/
│   ├── curated/
│   ├── quarantine/
│   └── analytics/
│
├── requirements.txt
├── .gitignore
└── README.md
```

The `data/raw`, `data/curated`, `data/quarantine`, and `data/analytics` directories are excluded from source control so large datasets and generated outputs are not committed to GitHub.

---

# Pipeline Design

## 1. Ingestion

`src/ingestion.py`

The ingestion layer is responsible for creating the Spark session and loading the NYC Yellow Taxi Parquet data.

Rather than combining January, February, and March into one large source file manually, Spark can read the files from the same directory as one DataFrame.

Conceptually:

```python
spark.read.parquet("data/raw/yellow_taxi/")
```

This allows the pipeline to work with monthly datasets while keeping the original files independent.

The same pattern can later be applied to an S3 prefix:

```text
s3://<bucket>/raw/yellow_taxi/
```

This provides a straightforward path from local development to AWS.

---

## 2. Data Quality Validation

`src/data_quality.py`

Incoming records are evaluated before they enter the curated dataset.

Records are quarantined when important analytical fields fail validation rules such as:

- missing pickup timestamp
- missing dropoff timestamp
- missing trip distance
- missing fare amount
- missing total amount
- trip distance less than or equal to zero
- negative fare amount
- negative total amount

Conceptually:

```text
Raw Data
   |
   v
Validation
 /       \
Valid   Rejected
 |         |
 v         v
ETL    Quarantine
```

The quarantine layer is important because rejected records are not silently deleted.

Instead, they can be retained separately for investigation, rule refinement, or future reprocessing.

Some unusual records are treated as **quality flags rather than automatic failures**.

Examples include:

```text
passenger_count <= 0
trip_distance > 100 miles
```

This distinction is intentional: unusual data is not automatically the same as invalid data.

---

## 3. Transformations

`src/transformations.py`

After validation, the pipeline creates additional fields useful for analytics.

Examples include:

```text
trip_duration_minutes
pickup_date
dropoff_date
pickup_year
pickup_month
pickup_day
pickup_hour
tip_percentage
```

For example:

```text
Pickup:   2025-01-15 10:00
Dropoff:  2025-01-15 10:30

trip_duration_minutes = 30
pickup_year = 2025
pickup_month = 1
pickup_hour = 10
```

Trips with zero or negative calculated duration are removed from the curated analytical dataset.

These derived columns allow downstream systems to answer business questions without repeatedly rebuilding the same transformation logic.

---

## 4. Taxi Zone Enrichment

`src/enrichment.py`

The TLC trip dataset stores pickup and dropoff locations primarily as numeric location IDs.

The pipeline joins those IDs to the NYC Taxi Zone Lookup dataset.

This converts fields such as:

```text
PULocationID = 161
```

into analytical attributes such as:

```text
pickup_borough
pickup_zone
pickup_service_zone
```

The same enrichment is performed for dropoff locations.

### Broadcast Join

The Taxi Zone Lookup is very small compared with the Yellow Taxi trip dataset.

For this reason, the pipeline uses a Spark broadcast join for the zone dimension.

```python
F.broadcast(pickup_zones)
```

Instead of shuffling the large taxi dataset across Spark partitions, the small lookup dataset can be distributed to Spark executors.

This is an example of designing the transformation around the relative size of the datasets rather than treating every join identically.

---

# Storage Design

The AWS-oriented storage structure separates datasets by their stage in the pipeline.

```text
s3://<bucket>/
│
├── raw/
│   └── yellow_taxi/
│
├── curated/
│   └── yellow_taxi/
│
└── quarantine/
    └── yellow_taxi/
```

## Raw Layer

Contains the original TLC source files.

The raw layer is intended to preserve source data without modifying it.

```text
raw/yellow_taxi/
```

Example:

```text
yellow_tripdata_2025-01.parquet
yellow_tripdata_2025-02.parquet
yellow_tripdata_2025-03.parquet
```

## Curated Layer

Contains cleaned, transformed, validated, and enriched records.

The curated dataset is stored as **Parquet** and partitioned by:

```text
pickup_year
pickup_month
```

Example layout:

```text
curated/yellow_taxi/
│
└── pickup_year=2025/
    ├── pickup_month=1/
    ├── pickup_month=2/
    └── pickup_month=3/
```

This structure allows query engines to avoid scanning unrelated monthly data when partition filters are used.

## Quarantine Layer

Contains records that fail the project's analytical quality rules.

```text
quarantine/yellow_taxi/
```

Keeping rejected records separate provides visibility into data-quality problems instead of silently discarding them.

---

# Why Parquet?

The pipeline uses Parquet rather than CSV for curated analytical data.

Parquet provides several advantages for analytical workloads:

- columnar storage
- compression
- schema information
- efficient Spark processing
- efficient analytical queries
- reduced unnecessary column reads
- compatibility with Athena, Glue, and Redshift workflows

This makes it a better format for the curated layer than repeatedly storing transformed data as CSV.

---

# Partitioning Strategy

The curated dataset is partitioned by:

```text
pickup_year
pickup_month
```

using PySpark:

```python
.partitionBy(
    "pickup_year",
    "pickup_month"
)
```

For example, a query targeting January 2025 can focus on:

```text
pickup_year=2025/
pickup_month=1/
```

instead of scanning every month in the dataset.

This design becomes increasingly valuable as additional monthly TLC files are introduced.

---

# Incremental Processing Design

The source data naturally arrives as monthly files.

Instead of designing the pipeline around one permanently combined dataset, the architecture supports monthly ingestion.

```text
January
   |
February
   |
March
   |
   v
raw/yellow_taxi/
   |
   v
PySpark
   |
   v
Year / Month Partitions
```

This allows future months to follow the same pipeline without changing the fundamental transformation logic.

A production extension of this project would add stronger idempotent processing so rerunning a monthly batch safely replaces or merges the target partition rather than creating duplicate records.

This is intentionally listed as a production enhancement rather than claiming the current local implementation already provides full orchestration and idempotency.

---

# Athena Analytics Layer

`sql/athena_queries.sql`

The Athena layer is designed to query curated Parquet directly from S3.

The SQL file includes the database/table definition and analytical queries.

Examples include:

- monthly trip volume
- monthly revenue
- revenue by pickup borough
- busiest pickup zones
- average trip distance
- average trip duration
- hourly taxi demand
- tip analysis
- daily revenue trends
- common pickup/dropoff routes
- top revenue-generating zones

The project also includes window-function examples.

### Zone Ranking

```sql
DENSE_RANK() OVER (
    PARTITION BY pickup_borough
    ORDER BY total_trips DESC
)
```

This ranks high-volume taxi zones within each borough.

### Month-over-Month Analysis

```sql
LAG(total_trips) OVER (
    ORDER BY pickup_year, pickup_month
)
```

This allows current-month activity to be compared with the previous month.

---

# Redshift Warehouse Design

`sql/redshift_queries.sql`

The Redshift portion of the project extends the pipeline from data-lake analytics into dimensional warehouse design.

The proposed warehouse contains:

```text
stg_yellow_taxi
       |
       +----------------+
       |                |
       v                v
   dim_date          dim_zone
       \                /
        \              /
         v            v
          fact_trips
```

## Staging Table

```text
stg_yellow_taxi
```

Represents the curated dataset before warehouse modeling.

## Fact Table

```text
fact_trips
```

Stores trip-level measures including:

- trip distance
- trip duration
- fare amount
- tip amount
- toll amount
- total amount
- passenger count
- pickup/dropoff keys

## Dimensions

```text
dim_date
dim_zone
```

These provide reusable descriptive attributes for analytical joins.

This enables warehouse queries such as:

- monthly revenue
- trip volume by zone
- borough revenue
- hourly demand
- common routes
- ranked zones
- month-over-month revenue growth

---

# Testing

`tests/test_transformations.py`

The project includes automated PySpark tests using `pytest`.

Current tests cover important transformation behavior such as:

### Data Quality

Validates that acceptable and rejected records are separated correctly.

### Feature Engineering

Verifies fields such as:

```text
trip_duration_minutes
pickup_year
pickup_month
pickup_day
pickup_hour
tip_percentage
```

are calculated correctly.

### Duration Validation

Verifies zero and negative trip durations do not enter the final analytical dataset.

Tests can be executed with:

```bash
pytest tests/
```

The goal is to validate transformation behavior independently from AWS infrastructure.

---

# Modular PySpark Design

Instead of placing the entire ETL workflow inside one large Python script, responsibilities are separated into modules.

```text
ingestion.py
      |
data_quality.py
      |
transformations.py
      |
enrichment.py
      |
pipeline.py
```

### `ingestion.py`

Handles Spark initialization and source ingestion.

### `data_quality.py`

Handles validation and quality flags.

### `transformations.py`

Handles cleaning and feature engineering.

### `enrichment.py`

Handles Taxi Zone Lookup enrichment.

### `pipeline.py`

Acts as the orchestration layer that connects the pipeline stages.

This separation makes the project easier to:

- understand
- test
- maintain
- debug
- extend
- migrate to AWS Glue

---

# Performance Considerations

Several design decisions were made with larger workloads in mind.

## Columnar Storage

Curated output uses Parquet instead of CSV.

## Partitioning

Data is partitioned by year and month to improve downstream filtering.

## Broadcast Join

The small Taxi Zone dimension is broadcast rather than requiring a large shuffle join.

## Native Spark Functions

Transformations use Spark DataFrame functions instead of moving processing into normal Python loops.

This keeps computation within Spark's execution engine.

## Modular Processing

Transformation stages are separated so individual parts of the pipeline can be tested and optimized independently.

---

# Local Development and AWS Deployment

The project follows a local-first development approach.

The PySpark pipeline is developed and tested locally before running the same processing logic using AWS Glue.

```text
Local Development
       |
       | test / debug
       v
PySpark Pipeline
       |
       | deploy
       v
AWS Glue
       |
       v
Amazon S3
```

This provides faster development feedback while avoiding unnecessary cloud compute during early development.

---

# Current Deployment Status

This repository intentionally distinguishes between components that have been developed and components that still require final AWS execution.

### Configured / Developed

- S3 raw, curated, and quarantine architecture
- NYC Yellow Taxi source data
- AWS Glue service role
- PySpark ingestion module
- data-quality validation
- transformation logic
- Taxi Zone enrichment
- partitioned-output design
- Athena SQL
- Redshift warehouse SQL
- PySpark unit tests
- architecture documentation

### Local Validation / Deployment Work

The PySpark code is being developed and tested locally before final Glue execution.

The AWS Glue environment was configured, but Glue compute execution was not completed during the initial development stage.

Athena and Redshift SQL are included as deployment-ready project components, but their final execution should be validated against the actual curated Parquet schema after the PySpark pipeline has completed successfully.

This distinction is intentional so the repository accurately represents what has been implemented, tested, configured, and designed.

---

# Running the Project Locally

## 1. Clone the Repository

```bash
git clone <repository-url>

cd nyc-yellow-taxi-data-pipeline
```

## 2. Install Dependencies

```bash
pip install -r requirements.txt
```

## 3. Add TLC Data

Place the monthly Parquet files in:

```text
data/raw/yellow_taxi/
```

For example:

```text
yellow_tripdata_2025-01.parquet
yellow_tripdata_2025-02.parquet
yellow_tripdata_2025-03.parquet
```

## 4. Add Taxi Zone Lookup

Place the Taxi Zone Lookup CSV in:

```text
data/reference/taxi_zone_lookup.csv
```

## 5. Run the Pipeline

```bash
spark-submit src/pipeline.py
```

Expected outputs are written to:

```text
data/curated/
data/quarantine/
data/analytics/
```

## 6. Run Tests

```bash
pytest tests/
```

---

# Data Engineering Concepts Demonstrated

This project demonstrates:

- ETL pipeline design
- PySpark DataFrames
- distributed transformations
- data-quality validation
- quarantine patterns
- feature engineering
- dimension enrichment
- broadcast joins
- Parquet storage
- partitioned datasets
- monthly ingestion patterns
- modular Python development
- automated testing
- Amazon S3 data-lake design
- AWS Glue architecture
- Glue Data Catalog integration
- Amazon Athena
- Amazon Redshift
- dimensional modeling
- fact and dimension tables
- analytical SQL
- CTEs
- window functions
- cloud deployment planning

---

# Future Improvements

The architecture can be extended with:

- automated monthly ingestion
- idempotent partition processing
- explicit Spark schema enforcement
- schema-drift detection
- detailed rejection reason tracking
- row-count reconciliation between pipeline stages
- CloudWatch monitoring and alerting
- workflow orchestration
- CI/CD testing
- Redshift Serverless deployment
- dashboard development with Tableau or QuickSight

A further production version could introduce orchestration and event-driven ingestion so new TLC files automatically trigger processing.

---

# Project Summary

This project demonstrates how raw monthly transportation data can be turned into an analytics-ready data platform.

The overall design follows:

```text
Raw Data
   ↓
Ingestion
   ↓
Validation
   ↓
Transformation
   ↓
Enrichment
   ↓
Curated Parquet
   ↓
Data Catalog
   ↓
Athena
   ↓
Redshift
   ↓
Analytics
```

The focus of the project is not only producing an output dataset, but demonstrating the engineering decisions involved in building a pipeline that is modular, testable, observable, cost-conscious, and capable of being extended as data volume and requirements grow.
