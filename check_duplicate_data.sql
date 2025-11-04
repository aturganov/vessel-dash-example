-- Query to check for duplicate data in f_data
-- This will help identify if there are any duplicate records

-- Basic count of total records
SELECT 'Total records in f_data' as description, COUNT(*) as count
FROM f_data;

-- Count of unique combinations (should equal total if no duplicates)
SELECT 'Unique combinations (ship_key, calendar_key)' as description,
       COUNT(DISTINCT ship_key, calendar_key) as count
FROM f_data;

-- Check for potential duplicates by counting occurrences
SELECT ship_key, calendar_key, COUNT(*) as duplicate_count
FROM f_data
GROUP BY ship_key, calendar_key
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC;

-- Additional check: see if multiple ships recorded on same dates
SELECT dc.date, dc.hour, dc.minute, COUNT(DISTINCT f.ship_key) as ship_count, COUNT(*) as total_records
FROM f_data f
JOIN d_calendar dc ON f.calendar_key = dc.calendar_key
GROUP BY dc.date, dc.hour, dc.minute
HAVING COUNT(DISTINCT f.ship_key) > 1
ORDER BY ship_count DESC;