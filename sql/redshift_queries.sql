-- =========================================================
-- AWS Setup
-- =========================================================
--
-- Region: us-east-2
--
-- S3 folders used for this project:
-- raw/yellow_taxi/
-- curated/yellow_taxi/
-- quarantine/yellow_taxi/
--
-- Glue role: AWSGlueServiceRole
--
-- I'm building and testing the PySpark pipeline locally first.
-- I already set up the S3 and Glue portions in AWS.
-- Redshift will be connected after the local pipeline is tested.
--
-- Before running the COPY command, I still need to add:
-- - my S3 bucket name
-- - my AWS account ID
-- - the IAM role I create for Redshift
--
-- I also need to check the final Parquet schema before
-- loading the curated data into Redshift.

-- =========================================================
-- 1. CREATE SCHEMA
-- =========================================================

CREATE SCHEMA IF NOT EXISTS nyc_taxi;



-- =========================================================
-- 2. CREATE STAGING TABLE
-- =========================================================
--
-- This table represents the curated PySpark output
-- before it is transformed into dimensional tables.
--
-- Final column types/order should be validated against
-- the actual Parquet schema before deployment.
-- =========================================================

CREATE TABLE IF NOT EXISTS nyc_taxi.stg_yellow_taxi (

    VendorID BIGINT,

    tpep_pickup_datetime TIMESTAMP,
    tpep_dropoff_datetime TIMESTAMP,

    passenger_count DOUBLE PRECISION,
    trip_distance DOUBLE PRECISION,

    RatecodeID DOUBLE PRECISION,

    store_and_fwd_flag VARCHAR(10),

    PULocationID BIGINT,
    DOLocationID BIGINT,

    payment_type BIGINT,

    fare_amount DOUBLE PRECISION,
    extra DOUBLE PRECISION,
    mta_tax DOUBLE PRECISION,
    tip_amount DOUBLE PRECISION,
    tolls_amount DOUBLE PRECISION,
    improvement_surcharge DOUBLE PRECISION,
    total_amount DOUBLE PRECISION,
    congestion_surcharge DOUBLE PRECISION,
    Airport_fee DOUBLE PRECISION,

    trip_duration_minutes DOUBLE PRECISION,

    pickup_date DATE,
    dropoff_date DATE,

    pickup_day INTEGER,
    pickup_hour INTEGER,

    tip_percentage DOUBLE PRECISION,

    pickup_borough VARCHAR(50),
    pickup_zone VARCHAR(100),
    pickup_service_zone VARCHAR(50),

    dropoff_borough VARCHAR(50),
    dropoff_zone VARCHAR(100),
    dropoff_service_zone VARCHAR(50),

    pickup_year INTEGER,
    pickup_month INTEGER

)

DISTSTYLE AUTO
SORTKEY (tpep_pickup_datetime);



-- =========================================================
-- 3. LOAD CURATED PARQUET DATA FROM S3
-- =========================================================
--
-- Redshift COPY can load Parquet files directly from S3.
--
-- IMPORTANT:
-- The Redshift staging table structure must match the
-- Parquet structure used for the load.
-- =========================================================

COPY nyc_taxi.stg_yellow_taxi

FROM
's3://<YOUR-BUCKET-NAME>/curated/yellow_taxi/'

IAM_ROLE
'arn:aws:iam::<YOUR-AWS-ACCOUNT-ID>:role/<YOUR-REDSHIFT-IAM-ROLE>'

FORMAT AS PARQUET;



-- =========================================================
-- 4. CREATE DATE DIMENSION
-- =========================================================

CREATE TABLE IF NOT EXISTS nyc_taxi.dim_date (

    date_key INTEGER,

    full_date DATE,

    year INTEGER,
    month INTEGER,
    day INTEGER,

    PRIMARY KEY (date_key)

)

DISTSTYLE ALL
SORTKEY (full_date);



-- =========================================================
-- 5. CREATE ZONE DIMENSION
-- =========================================================

CREATE TABLE IF NOT EXISTS nyc_taxi.dim_zone (

    location_id BIGINT,

    borough VARCHAR(50),
    zone_name VARCHAR(100),
    service_zone VARCHAR(50),

    PRIMARY KEY (location_id)

)

DISTSTYLE ALL;



-- =========================================================
-- 6. CREATE FACT TABLE
-- =========================================================

CREATE TABLE IF NOT EXISTS nyc_taxi.fact_trips (

    trip_id BIGINT
        IDENTITY(1,1),

    pickup_datetime TIMESTAMP,
    dropoff_datetime TIMESTAMP,

    pickup_date_key INTEGER,
    dropoff_date_key INTEGER,

    pickup_location_id BIGINT,
    dropoff_location_id BIGINT,

    vendor_id BIGINT,
    payment_type BIGINT,

    passenger_count DOUBLE PRECISION,

    trip_distance DOUBLE PRECISION,
    trip_duration_minutes DOUBLE PRECISION,

    fare_amount DOUBLE PRECISION,
    tip_amount DOUBLE PRECISION,
    tolls_amount DOUBLE PRECISION,
    total_amount DOUBLE PRECISION,

    tip_percentage DOUBLE PRECISION,

    pickup_hour INTEGER,

    PRIMARY KEY (trip_id)

)

DISTSTYLE AUTO
SORTKEY (pickup_datetime);



-- =========================================================
-- 7. POPULATE DATE DIMENSION
-- =========================================================

INSERT INTO nyc_taxi.dim_date

SELECT DISTINCT

    CAST(
        TO_CHAR(pickup_date, 'YYYYMMDD')
        AS INTEGER
    ) AS date_key,

    pickup_date AS full_date,

    EXTRACT(YEAR FROM pickup_date)::INTEGER AS year,
    EXTRACT(MONTH FROM pickup_date)::INTEGER AS month,
    EXTRACT(DAY FROM pickup_date)::INTEGER AS day

FROM nyc_taxi.stg_yellow_taxi

WHERE pickup_date IS NOT NULL;



-- =========================================================
-- 8. POPULATE PICKUP ZONES
-- =========================================================

INSERT INTO nyc_taxi.dim_zone

SELECT DISTINCT

    PULocationID AS location_id,

    pickup_borough AS borough,
    pickup_zone AS zone_name,
    pickup_service_zone AS service_zone

FROM nyc_taxi.stg_yellow_taxi

WHERE PULocationID IS NOT NULL;



-- =========================================================
-- 9. POPULATE DROPOFF ZONES NOT ALREADY PRESENT
-- =========================================================

INSERT INTO nyc_taxi.dim_zone

SELECT DISTINCT

    s.DOLocationID,

    s.dropoff_borough,
    s.dropoff_zone,
    s.dropoff_service_zone

FROM nyc_taxi.stg_yellow_taxi s

WHERE s.DOLocationID IS NOT NULL

AND NOT EXISTS (

    SELECT 1

    FROM nyc_taxi.dim_zone z

    WHERE z.location_id = s.DOLocationID

);



-- =========================================================
-- 10. POPULATE FACT TABLE
-- =========================================================

INSERT INTO nyc_taxi.fact_trips (

    pickup_datetime,
    dropoff_datetime,

    pickup_date_key,
    dropoff_date_key,

    pickup_location_id,
    dropoff_location_id,

    vendor_id,
    payment_type,

    passenger_count,

    trip_distance,
    trip_duration_minutes,

    fare_amount,
    tip_amount,
    tolls_amount,
    total_amount,

    tip_percentage,

    pickup_hour
)

SELECT

    tpep_pickup_datetime,
    tpep_dropoff_datetime,

    CAST(
        TO_CHAR(pickup_date, 'YYYYMMDD')
        AS INTEGER
    ),

    CAST(
        TO_CHAR(dropoff_date, 'YYYYMMDD')
        AS INTEGER
    ),

    PULocationID,
    DOLocationID,

    VendorID,
    payment_type,

    passenger_count,

    trip_distance,
    trip_duration_minutes,

    fare_amount,
    tip_amount,
    tolls_amount,
    total_amount,

    tip_percentage,

    pickup_hour

FROM nyc_taxi.stg_yellow_taxi;



-- =========================================================
-- ANALYTICAL QUERIES
-- =========================================================



-- ---------------------------------------------------------
-- 11. MONTHLY TRIPS AND REVENUE
-- ---------------------------------------------------------

SELECT

    d.year,
    d.month,

    COUNT(*) AS total_trips,

    ROUND(
        SUM(f.total_amount),
        2
    ) AS total_revenue,

    ROUND(
        AVG(f.total_amount),
        2
    ) AS avg_trip_value

FROM nyc_taxi.fact_trips f

JOIN nyc_taxi.dim_date d
    ON f.pickup_date_key = d.date_key

GROUP BY
    d.year,
    d.month

ORDER BY
    d.year,
    d.month;



-- ---------------------------------------------------------
-- 12. TOP PICKUP ZONES
-- ---------------------------------------------------------

SELECT

    z.borough,
    z.zone_name,

    COUNT(*) AS total_trips,

    ROUND(
        SUM(f.total_amount),
        2
    ) AS total_revenue

FROM nyc_taxi.fact_trips f

JOIN nyc_taxi.dim_zone z
    ON f.pickup_location_id = z.location_id

GROUP BY
    z.borough,
    z.zone_name

ORDER BY total_trips DESC

LIMIT 20;



-- ---------------------------------------------------------
-- 13. REVENUE BY BOROUGH
-- ---------------------------------------------------------

SELECT

    z.borough,

    COUNT(*) AS total_trips,

    ROUND(
        SUM(f.total_amount),
        2
    ) AS total_revenue,

    ROUND(
        AVG(f.total_amount),
        2
    ) AS avg_trip_value

FROM nyc_taxi.fact_trips f

JOIN nyc_taxi.dim_zone z
    ON f.pickup_location_id = z.location_id

GROUP BY z.borough

ORDER BY total_revenue DESC;



-- ---------------------------------------------------------
-- 14. HOURLY TAXI DEMAND
-- ---------------------------------------------------------

SELECT

    pickup_hour,

    COUNT(*) AS total_trips,

    ROUND(
        AVG(total_amount),
        2
    ) AS avg_trip_value

FROM nyc_taxi.fact_trips

GROUP BY pickup_hour

ORDER BY pickup_hour;



-- ---------------------------------------------------------
-- 15. MOST COMMON TAXI ROUTES
-- ---------------------------------------------------------

SELECT

    pickup.zone_name AS pickup_zone,

    dropoff.zone_name AS dropoff_zone,

    COUNT(*) AS total_trips,

    ROUND(
        AVG(f.trip_distance),
        2
    ) AS avg_trip_distance,

    ROUND(
        AVG(f.total_amount),
        2
    ) AS avg_trip_value

FROM nyc_taxi.fact_trips f

JOIN nyc_taxi.dim_zone pickup
    ON f.pickup_location_id = pickup.location_id

JOIN nyc_taxi.dim_zone dropoff
    ON f.dropoff_location_id = dropoff.location_id

GROUP BY
    pickup.zone_name,
    dropoff.zone_name

ORDER BY total_trips DESC

LIMIT 20;



-- ---------------------------------------------------------
-- 16. RANK ZONES WITHIN EACH BOROUGH
-- ---------------------------------------------------------

WITH zone_activity AS (

    SELECT

        z.borough,
        z.zone_name,

        COUNT(*) AS total_trips

    FROM nyc_taxi.fact_trips f

    JOIN nyc_taxi.dim_zone z
        ON f.pickup_location_id = z.location_id

    GROUP BY
        z.borough,
        z.zone_name

),

ranked_zones AS (

    SELECT

        borough,
        zone_name,
        total_trips,

        DENSE_RANK() OVER (

            PARTITION BY borough
            ORDER BY total_trips DESC

        ) AS zone_rank

    FROM zone_activity

)

SELECT

    borough,
    zone_name,
    total_trips,
    zone_rank

FROM ranked_zones

WHERE zone_rank <= 3

ORDER BY
    borough,
    zone_rank;



-- ---------------------------------------------------------
-- 17. MONTH-OVER-MONTH REVENUE GROWTH
-- ---------------------------------------------------------

WITH monthly_revenue AS (

    SELECT

        d.year,
        d.month,

        SUM(f.total_amount) AS revenue

    FROM nyc_taxi.fact_trips f

    JOIN nyc_taxi.dim_date d
        ON f.pickup_date_key = d.date_key

    GROUP BY
        d.year,
        d.month

),

revenue_comparison AS (

    SELECT

        year,
        month,
        revenue,

        LAG(revenue) OVER (

            ORDER BY
                year,
                month

        ) AS previous_month_revenue

    FROM monthly_revenue

)

SELECT

    year,
    month,

    ROUND(revenue, 2)
        AS revenue,

    ROUND(previous_month_revenue, 2)
        AS previous_month_revenue,

    ROUND(

        (
            revenue - previous_month_revenue
        )

        * 100.0

        / NULLIF(
            previous_month_revenue,
            0
        ),

        2

    ) AS revenue_growth_pct

FROM revenue_comparison

ORDER BY
    year,
    month;
