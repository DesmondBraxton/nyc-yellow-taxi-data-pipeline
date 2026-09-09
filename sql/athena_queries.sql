-- =========================================================
-- NYC Yellow Taxi Data Pipeline
-- Athena Analytics Queries
-- =========================================================
--
-- Assumes the curated dataset has been registered
-- in the AWS Glue Data Catalog and queried through Athena.
--
-- Replace:
-- nyc_taxi_db.curated_yellow_taxi
-- with the actual database and table name after deployment.
-- =========================================================


-- ---------------------------------------------------------
-- 1. MONTHLY TRIP VOLUME
-- ---------------------------------------------------------

SELECT
    pickup_year,
    pickup_month,
    COUNT(*) AS total_trips
FROM nyc_taxi_db.curated_yellow_taxi
GROUP BY
    pickup_year,
    pickup_month
ORDER BY
    pickup_year,
    pickup_month;


-- ---------------------------------------------------------
-- 2. MONTHLY REVENUE
-- ---------------------------------------------------------

SELECT
    pickup_year,
    pickup_month,
    ROUND(SUM(total_amount), 2) AS total_revenue,
    ROUND(AVG(total_amount), 2) AS avg_trip_revenue
FROM nyc_taxi_db.curated_yellow_taxi
GROUP BY
    pickup_year,
    pickup_month
ORDER BY
    pickup_year,
    pickup_month;


-- ---------------------------------------------------------
-- 3. REVENUE BY PICKUP BOROUGH
-- ---------------------------------------------------------

SELECT
    pickup_borough,
    COUNT(*) AS total_trips,
    ROUND(SUM(total_amount), 2) AS total_revenue,
    ROUND(AVG(total_amount), 2) AS avg_trip_value
FROM nyc_taxi_db.curated_yellow_taxi
WHERE pickup_borough IS NOT NULL
GROUP BY pickup_borough
ORDER BY total_revenue DESC;


-- ---------------------------------------------------------
-- 4. TOP 10 BUSIEST PICKUP ZONES
-- ---------------------------------------------------------

SELECT
    pickup_borough,
    pickup_zone,
    COUNT(*) AS total_pickups
FROM nyc_taxi_db.curated_yellow_taxi
WHERE pickup_zone IS NOT NULL
GROUP BY
    pickup_borough,
    pickup_zone
ORDER BY total_pickups DESC
LIMIT 10;


-- ---------------------------------------------------------
-- 5. AVERAGE TRIP DISTANCE AND DURATION BY BOROUGH
-- ---------------------------------------------------------

SELECT
    pickup_borough,
    ROUND(AVG(trip_distance), 2) AS avg_trip_distance,
    ROUND(AVG(trip_duration_minutes), 2) AS avg_trip_duration_minutes
FROM nyc_taxi_db.curated_yellow_taxi
WHERE pickup_borough IS NOT NULL
GROUP BY pickup_borough
ORDER BY avg_trip_distance DESC;


-- ---------------------------------------------------------
-- 6. HOURLY TAXI DEMAND
-- ---------------------------------------------------------

SELECT
    pickup_hour,
    COUNT(*) AS total_trips,
    ROUND(AVG(total_amount), 2) AS avg_trip_value
FROM nyc_taxi_db.curated_yellow_taxi
GROUP BY pickup_hour
ORDER BY pickup_hour;


-- ---------------------------------------------------------
-- 7. TIP ANALYSIS BY PAYMENT TYPE
-- ---------------------------------------------------------

SELECT
    payment_type,
    COUNT(*) AS total_trips,
    ROUND(AVG(tip_amount), 2) AS avg_tip_amount,
    ROUND(AVG(tip_percentage), 2) AS avg_tip_percentage
FROM nyc_taxi_db.curated_yellow_taxi
GROUP BY payment_type
ORDER BY avg_tip_percentage DESC;


-- ---------------------------------------------------------
-- 8. DAILY TRIP AND REVENUE TREND
-- ---------------------------------------------------------

SELECT
    pickup_date,
    COUNT(*) AS total_trips,
    ROUND(SUM(total_amount), 2) AS daily_revenue,
    ROUND(AVG(trip_distance), 2) AS avg_trip_distance,
    ROUND(AVG(trip_duration_minutes), 2) AS avg_trip_duration_minutes
FROM nyc_taxi_db.curated_yellow_taxi
GROUP BY pickup_date
ORDER BY pickup_date;


-- ---------------------------------------------------------
-- 9. MOST COMMON PICKUP TO DROPOFF ROUTES
-- ---------------------------------------------------------

SELECT
    pickup_zone,
    dropoff_zone,
    COUNT(*) AS total_trips,
    ROUND(AVG(trip_distance), 2) AS avg_trip_distance,
    ROUND(AVG(total_amount), 2) AS avg_trip_value
FROM nyc_taxi_db.curated_yellow_taxi
WHERE pickup_zone IS NOT NULL
  AND dropoff_zone IS NOT NULL
GROUP BY
    pickup_zone,
    dropoff_zone
ORDER BY total_trips DESC
LIMIT 20;


-- ---------------------------------------------------------
-- 10. WINDOW FUNCTION:
-- RANK PICKUP ZONES WITHIN EACH BOROUGH
-- ---------------------------------------------------------

WITH zone_trips AS (
    SELECT
        pickup_borough,
        pickup_zone,
        COUNT(*) AS total_trips
    FROM nyc_taxi_db.curated_yellow_taxi
    WHERE pickup_borough IS NOT NULL
      AND pickup_zone IS NOT NULL
    GROUP BY
        pickup_borough,
        pickup_zone
),

ranked_zones AS (
    SELECT
        pickup_borough,
        pickup_zone,
        total_trips,
        DENSE_RANK() OVER (
            PARTITION BY pickup_borough
            ORDER BY total_trips DESC
        ) AS zone_rank
    FROM zone_trips
)

SELECT
    pickup_borough,
    pickup_zone,
    total_trips,
    zone_rank
FROM ranked_zones
WHERE zone_rank <= 3
ORDER BY
    pickup_borough,
    zone_rank;


-- ---------------------------------------------------------
-- 11. WINDOW FUNCTION:
-- MONTH-OVER-MONTH TRIP GROWTH
-- ---------------------------------------------------------

WITH monthly_trips AS (
    SELECT
        pickup_year,
        pickup_month,
        COUNT(*) AS total_trips
    FROM nyc_taxi_db.curated_yellow_taxi
    GROUP BY
        pickup_year,
        pickup_month
),

monthly_comparison AS (
    SELECT
        pickup_year,
        pickup_month,
        total_trips,
        LAG(total_trips) OVER (
            ORDER BY pickup_year, pickup_month
        ) AS previous_month_trips
    FROM monthly_trips
)

SELECT
    pickup_year,
    pickup_month,
    total_trips,
    previous_month_trips,
    ROUND(
        (
            total_trips - previous_month_trips
        ) * 100.0
        / NULLIF(previous_month_trips, 0),
        2
    ) AS month_over_month_growth_pct
FROM monthly_comparison
ORDER BY
    pickup_year,
    pickup_month;


-- ---------------------------------------------------------
-- 12. TOP REVENUE-GENERATING ZONES
-- ---------------------------------------------------------

SELECT
    pickup_borough,
    pickup_zone,
    COUNT(*) AS total_trips,
    ROUND(SUM(total_amount), 2) AS total_revenue
FROM nyc_taxi_db.curated_yellow_taxi
WHERE pickup_zone IS NOT NULL
GROUP BY
    pickup_borough,
    pickup_zone
ORDER BY total_revenue DESC
LIMIT 20;
