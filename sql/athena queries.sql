-- ============================================================
-- 1. TOP CHANNELS BY DEDUPLICATED VIEWS (2023)
-- Purpose:
-- Remove duplicate trending appearances and calculate
-- actual channel performance.
-- ============================================================

WITH latest_video_views AS (

SELECT
video_id,
channelTitle,
MAX(view_count) AS final_views
FROM youtube_etl_db.fact_trending_videos
WHERE trending_year = 2023
GROUP BY
video_id,
channelTitle

)

SELECT
channelTitle,
SUM(final_views) AS total_views,
COUNT(video_id) AS unique_videos
FROM latest_video_views
GROUP BY channelTitle
ORDER BY total_views DESC
LIMIT 10;

-- ============================================================
-- 2. TOP VIDEOS BY VIEW COUNT
-- Purpose:
-- Identify the highest performing videos.
-- ============================================================

SELECT
title,
channelTitle,
view_count
FROM youtube_etl_db.fact_trending_videos
ORDER BY view_count DESC
LIMIT 10;

-- ============================================================
-- 3. CATEGORY TRENDING DURATION ANALYSIS
-- Purpose:
-- Find categories that remain trending longer.
-- ============================================================

SELECT
categoryid,
AVG(trending_days) AS avg_trending_days
FROM youtube_etl_db.fact_trending_videos
GROUP BY categoryid
ORDER BY avg_trending_days DESC;

-- ============================================================
-- 4. COUNTRY DISTRIBUTION CHECK
-- Purpose:
-- Validate loaded records by country.
-- ============================================================

SELECT
country,
COUNT(*) AS total_rows
FROM youtube_etl_db.fact_trending_videos
GROUP BY country
ORDER BY total_rows DESC;

-- ============================================================
-- 5. DUPLICATE RECORD DETECTION
-- Purpose:
-- Verify ETL data quality.
-- ============================================================

SELECT
video_id,
categoryid,
country,
trending_date,
COUNT(*) AS duplicate_count
FROM youtube_etl_db.fact_trending_videos
GROUP BY
video_id,
categoryid,
country,
trending_date
HAVING COUNT(*) > 1;

-- ============================================================
-- 6. TOTAL DUPLICATE GROUP COUNT
-- Purpose:
-- Summary of duplicate detection.
-- ============================================================

SELECT
COUNT(*) AS duplicate_groups
FROM (

SELECT
video_id,
country,
trending_date,
COUNT(*) AS cnt
FROM youtube_etl_db.fact_trending_videos
GROUP BY
video_id,
country,
trending_date
HAVING COUNT(*) > 1

);

-- ============================================================
-- 7. ENGAGEMENT ANALYSIS BY CATEGORY
-- Purpose:
-- Compare category engagement performance.
-- ============================================================

SELECT
categoryid,
ROUND(AVG(engagement_rate),4) AS avg_engagement,
ROUND(AVG(like_ratio),4) AS avg_like_ratio
FROM youtube_etl_db.fact_trending_videos
GROUP BY categoryid
ORDER BY avg_engagement DESC;
