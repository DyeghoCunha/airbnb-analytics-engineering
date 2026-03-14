{{ config(
  materialized = 'table',
  tags = ['silver','staging']
) }}

WITH source AS (

  SELECT
    *
  FROM
    {{ source(
      'airbnb_raw',
      'raw_listings'
    ) }}
),
decuplicated AS(
  SELECT
    *,
    ROW_NUMBER() over (
      PARTITION BY id
      ORDER BY
        updated_at DESC,
        created_at DESC
    ) AS rn
  FROM
    source
),
price_cleaned AS(
  SELECT
    *,
    CAST(
      REGEXP_REPLACE(REGEXP_REPLACE(price, '[^0-9.]', '') '^$', '0') AS DECIMAL(
        10,
        2
      )
    ) AS price
  FROM
    deduplicated
  WHERE
    rn = 1
    AND id IS NOT NULL
),
cleaned AS (id AS listing_id, TRIM(listing_url) AS listing_url, TRIM(NAME) AS NAME, CASE
WHEN LOWER(TRIM(room_type)) LIKE '%entire%' THEN 'Entire Home/apt'
WHEN LOWER(TRIM(room_type)) LIKE '%private%' THEN 'Private Room'
WHEN LOWER(TRIM(room_type)) LIKE '%shared%' THEN 'Shared Room'
WHEN LOWER(TRIM(room_type)) LIKE '%hotel%' THEN 'Hotel Room'
ELSE 'Other'END AS room_type, host_id, price, CASE
WHEN minimum_nights <= 0 THEN 1
WHEN minimum_nights > 365 THEN 365
ELSE CAST(minimum_nights AS INTEGER)END AS minimum_nights, CASE
WHEN price < 50 THEN 'Budget'
WHEN price BETWEEN 50
AND 150 THEN 'Mid-Range'
WHEN price BETWEEN 151
AND 300 THEN 'Premium'
WHEN price > 300 THEN 'Luxury'
ELSE 'Unknown'END AS price_category, CASE
WHEN minimum_nights <= 1 THEN 'Flexible'
WHEN minimum_nights BETWEEN 2
AND 7 THEN 'Short Stay'
WHEN minimum_nights BETWEEN 8
AND 30 THEN 'Medium Stay'
WHEN minimum_nights > 30 THEN 'Long Stay'
ELSE 'Unknown'END AS stay_type, CASE
WHEN price > 0
AND minimum_nights > 0 THEN TRUE
ELSE FALSEEND AS is_bookable, CAST(created_at AS TIMESTAMP) AS listing_created_at, CAST(updated_at AS TIMESTAMP) AS listing_updated_at, CURRENT_TIMESTAMP() AS dbt_loaded_at, 'stg_listing' AS dbt_source_model
FROM
  price_cleaned
WHERE
  id IS NOT NULL
  AND host_id IS NOT NULL)
SELECT
  *
FROM
  cleaned
