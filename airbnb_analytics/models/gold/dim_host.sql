{{ config(
  materialized = 'table',
  tags = ['gold','dimension']
) }}

WITH stg_hosts AS (

  SELECT
    *
  FROM
    {{ ref('stg_hosts') }}
),
--! agregar listing por host 
host_listings AS (
  SELECT
    host_id,
    COUNT(
      DISTINCT listing_id
    ) AS total_listings,
    AVG(price) AS avg_listing_price,
    MIN(price) AS min_listing_price,
    MAX(price) AS max_listing_price,
    COUNT(
      DISTINCT CASE
        WHEN is_bookable = TRUE THEN listing_id
      END
    ) AS bookable_listings,
    COUNT(
      CASE
        WHEN room_type = 'Entire Home/apt' THEN 1
      END
    ) AS count_entire_home,
    COUNT(
      CASE
        WHEN room_type = 'Private Room' THEN 1
      END
    ) AS count_private_room,
    COUNT(
      CASE
        WHEN room_type = 'Shared Room' THEN 1
      END
    ) AS count_shared_room
  FROM
    {{ ref('stg_listings') }}
  GROUP BY
    host_id
),
host_reviews AS (
  SELECT
    l.host_id,
    COUNT(
      DISTINCT r.review_date
    ) AS total_reviews,
    COUNT(
      DISTINCT r.listing_id
    ) AS listings_with_reviews,
    ROUND(AVG(r.review_length), 2) AS avg_review_length,
    COUNT(
      CASE
        WHEN r.sentiment_category = 'Positive' THEN 1
      END
    ) AS positive_reviews,
    COUNT(
      CASE
        WHEN r.sentiment_category = 'Negative' THEN 1
      END
    ) AS negative_reviews,
    COUNT(
      CASE
        WHEN r.sentiment_category = 'Neutral' THEN 1
      END
    ) AS neutral_reviews,
    MIN(
      r.review_date
    ) AS first_review_date,
    MAX(
      r.review_date
    ) AS last_review_date
  FROM
    {{ ref('stg_reviews') }}
    r
    JOIN {{ ref('stg_listings') }}
    l
    ON r.listing_id = l.listing_id
  GROUP BY
    l.host_id
),
FINAL AS(
  SELECT
    {{ dbt_utils.generate_surrogate_key(['h.host_id']) }} AS host_key,
    h.host_id,
    h.host_name,
    h.is_superhost,
    h.host_experience_level,
    h.days_as_host,
    h.host_created_at,
    h.host_updated_at,
    COALESCE(
      hl.total_listings,
      0
    ) AS total_listings,
    COALESCE(
      hl.bookable_listings,
      0
    ) AS bookable_listings,
    COALESCE(
      hl.avg_listing_price,
      0
    ) AS avg_listing_price,
    COALESCE(
      hl.min_listing_price,
      0
    ) AS min_listing_price,
    COALESCE(
      hl.max_listing_price,
      0
    ) AS max_listing_price,
    COALESCE(
      hl.count_entire_home,
      0
    ) AS couint_entire_home,
    COALESCE(
      hl.count_private_room,
      0
    ) AS count_private_room,
    COALESCE(
      hl.count_shared_room,
      0
    ) AS count_shared_room,
    COALESCE(
      hr.total_reviews,
      0
    ) AS total_reviews,
    COALESCE(
      hr.listings_with_reviews,
      0
    ) AS listings_with_reviews,
    COALESCE(
      hr.avg_review_length,
      0
    ) AS avg_review_length,
    COALESCE(
      hr.positive_reviews,
      0
    ) AS positive_reviews,
    COALESCE(
      hr.negative_reviews,
      0
    ) AS negative_reviews,
    COALESCE(
      hr.neutral_reviews,
      0
    ) AS neutral_reviews,
    hr.first_review_date,
    hr.last_review_date,
    CASE
      WHEN COALESCE(
        hr.total_reviews,
        0
      ) > 0 THEN ROUND(COALESCE(hr.positive_reviews, 0) * 100.0 / NULLIF(hr.total_reviews, 0), 2)
      ELSE 0
    END AS positive_review_rate,
    CASE
      WHEN COALESCE(
        hl.total_listings,
        0
      ) > 0 THEN ROUND(COALESCE(hr.listings_with_reviews, 0) * 100.0 / NULLIF(hl.total_listings, 0), 2)
      ELSE 0
    END AS reviews_coverage_rate,
    CASE
      WHEN h.is_superhost = TRUE
      AND COALESCE(
        hr.total_reviews,
        0
      ) > 50 THEN 'Elite Host'
      WHEN h.is_superhost = TRUE THEN 'Superhost'
      WHEN COALESCE(
        hr.total_reviews,
        0
      ) > 100 THEN 'High-Volume Host'
      WHEN COALESCE(
        hr.total_reviews,
        0
      ) BETWEEN 10
      AND 100 THEN 'Active Host'
      WHEN COALESCE(
        hr.total_reviews,
        0
      ) < 10 THEN 'New/Low Activity Host'
      ELSE 'Unknown'
    END AS host_performance_tier,
    TRUE AS is_current,
    h.dbt_loaded_at AS effective_date,
    NULL :: TIMESTAMP AS end_date,
    CURRENT_TIMESTAMP() AS dbt_update_at
  FROM
    stg_hosts h
    LEFT JOIN host_listings hl
    ON h.host_id = hl.host_id
    LEFT JOIN host_reviews hr
    ON h.host_id = hr.host_id
)
SELECT
  *
FROM
  FINAL
