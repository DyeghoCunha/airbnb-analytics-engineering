{{ config(
  materialized = 'table',
  tags = ['silver','staging']
) }}

WITH source AS (
  SELECT * FROM {{ source('airbnb_raw', 'raw_hosts')}}
)

deduplicated AS (
  SELECT *,
  ROW_NUMBER() OVER ( Partition BY id ORDER BY updated_at DESC, created_at DESC) AS rn
   FROM source 
),

cleaned AS (
  SELECT  
  id AS host_id,
  TRIM(UPPER(name)) AS host_name,
  CASE
    WHEN LOWER(TRIM(is_superhost)) IN ('t','true','yes','1') THEN TRUE
    WHEN LOWER(TRIM(is_superhost)) IN ('f','false','no','0') THEN FALSE
    ELSE NULL
  END AS is_superhost,
  CAST (created_at AS TIMESTAMP) AS host_created_at,
  CAST (updated_at AS TIMESTAMP) AS host_updated_at,
  DATEDIFF(DAY, CAST(created_at AS TIMESTAMP), CURRENT_TIMESTAMP()) AS days_as_host,

  CASE 
    WHEN DATEDIFF(DAY, CAST(created_at AS TIMESTAMP), CURRENT_TIMESTAMP()) < 90 THEN 'New Host'
    WHEN DATEDIFF(DAY,CAST(created_at AS TIMESTAMP), CURRENT_TIMESTAMP()) BETWEEN 90 AND 365 THEN 'Intermediate'
    WHEN DATEDIFF(DAY, CAST(created_at AS TIMESTAMP), CURRENT_TIMESTAMP()) > 365 THEN 'Experienced'
    ELSE 'Unknown'
  END AS host_experience_level,

  CURRENT_TIMESTAMP() AS dbt_loaded_at,
  'stg_hosts' AS dbt_source_model

   FROM deduplicated WHERE rn = 1 AND id IS NOT NULL
)

SELECT * FROM cleaned