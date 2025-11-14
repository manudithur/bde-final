-- Quick check script to see what data exists
-- Check if scheduled_trips_mdb has data
SELECT COUNT(*) as scheduled_trips_count, 
       MIN(date) as min_date, 
       MAX(date) as max_date,
       COUNT(DISTINCT date) as unique_dates
FROM scheduled_trips_mdb;

-- Check if stadium_trip_intervals exists and has data
SELECT COUNT(*) as stadium_intervals_count
FROM stadium_trip_intervals;

-- Check available service dates
SELECT COUNT(*) as service_dates_count,
       MIN(date) as min_date,
       MAX(date) as max_date
FROM service_dates;

-- Check a sample of available dates
SELECT date, COUNT(*) as trips_count
FROM scheduled_trips_mdb
GROUP BY date
ORDER BY date
LIMIT 10;

