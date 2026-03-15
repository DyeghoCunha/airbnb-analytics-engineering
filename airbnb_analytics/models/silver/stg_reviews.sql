{{
  config(
    materialized = 'table',
    tags = ['silver','staging']
  )
}}

WITH source AS (
  SELECT * FROM {{ source('airbnb_raw','raw_reviews')}}
),

deduplicated AS (
  SELECT *,
  ROW_NUMBER() OVER(
    PARTITION BY listing_id, date, reviewer_name
    ORDER BY date
  ) AS rn
  FROM source
),
cleaned AS (
SELECT 
listing_id,
CAST(date AS DATE) AS review_date,
TRIM(reviewer_name) AS reviewer_name,
TRIM(comments) AS review_comments,
LENGTH(TRIM(comments)) AS review_length,
SIZE(SPLIT(TRIM(comments),' ')) AS word_count,
CASE
  WHEN LOWER(TRIM(sentiment)) IN ('positive','pos','good') THEN 'Positive'
  WHEN LOWER(TRIM(sentiment)) IN ('negative', 'neg','bad') THEN 'Negative'
  WHEN LOWER(TRIM(sentiment)) IN ('neutral','mixed') THEN 'Neutral'
  ELSE 'Unknown'
END AS sentiment_category,

CASE 
  WHEN LOWER(comments) LIKE '%clean%' THEN TRUE
  ELSE FALSE
END AS mentions_clean,

CASE
  WHEN LOWER(comments) LIKE '%location' THEN TRUE
  ELSE FALSE
END AS mentions_location,

CASE
  WHEN LOWER(comments) LIKE '%host%' THEN TRUE
  ELSE FALSE
END AS mentions_host,

YEAR(CAST(date AS DATE)) AS review_year,
MONTH(CAST(date AS DATE)) AS review_month,
QUARTER(CAST(date AS DATE)) AS review_quarter,
DAYOFWEEK(CAST(date AS DATE)) AS review_day_of_week,

CASE
  WHEN LENGTH(TRIM(comments))< 50 THEN "Very Short"
  WHEN LENGTH(TRIM(comments)) BETWEEN 50 AND 150 THEN 'Short'
  WHEN LENGTH(TRIM(comments)) BETWEEN 151 AND 300 THEN 'Medium'
  WHEN LENGTH(TRIM(comments)) > 300 THEN 'Long'
  ELSE 'Unknown'
END AS review_length_category,

CURRENT_TIMESTAMP() AS dbt_loaded_at,
'stg_reviews' AS dbt_source_model

FROM deduplicated WHERE rn = 1 AND listing_id IS NOT NULL
AND date IS NOT NULL
AND comments IS NOT NULL
AND TRIM(comments) != ''
)

SELECT * FROM cleaned