{{ config(
  materialized = 'table',
  tags = ['gold','dimension']
) }}

WITH stg_listings AS (

  SELECT
    *
  FROM
    {{ ref('stg_listings') }}
),
listing_reviews AS (
  SELECT
    listing_id,
    COUNT(*) AS total_reviews,
    COUNT(
      CASE
        WHEN sentiment_category = 'Positive' THEN 1
      END
    ) AS positive_reviews,
    COUNT(
      CASE
        WHEN sentiment_category = 'Negative' THEN 1
      END
    ) AS negative_reviews,
    AVG(review_length) AS avg_review_length,
    MIN(review_date) AS first_review_date,
    MAX(review_date) AS last_review_date,
    DATEDIFF(DAY, MAX(review_date), CURRENT_DATE()) AS days_since_last_review
  FROM
    {{ ref('stg_reviews') }}
  GROUP BY
    listing_id
),
FINAL AS (
  SELECT
    {{ dbt_utils.generate_surrogate_key(['l.listing_id']) }} AS listing_key,
    l.listing_id,
    l.host_id,
    l.listing_name,
    l.listing_url,
    l.room_type,
    l.price,
    l.minimum_nights,
    l.price_category,
    l.stay_type,
    l.is_bookable,
    l.listing_created_at,
    l.listing_updated_at,
    COALESCE(
      lr.total_reviews,
      0
    ) AS total_reviews,
    COALESCE(
      lr.positive_reviews,
      0
    ) AS positive_reviews,
    COALESCE(
      lr.negative_reviews,
      0
    ) AS negative_reviews,
    COALESCE(
      lr.avg_review_length,
      0
    ) AS avg_review_length,
    lr.first_review_date,
    lr.last_review_date,
    COALESCE(
      lr.days_since_last_review,
      9999
    ) AS days_since_last_review,
    CASE
      WHEN COALESCE(
        lr.total_reviews,
        0
      ) > 0 THEN ROUND(COALESCE(lr.positive_reviews, 0) * 100.0 / NULLIF(lr.total_reviews, 0), 2)
      ELSE 0
    END AS positive_review_rate,
    CASE
      WHEN COALESCE(
        lr.total_reviews,
        0
      ) > 100 THEN 'Very Popular'
      WHEN COALESCE(
        lr.total_reviews,
        0
      ) BETWEEN 50
      AND 100 THEN 'Popular'
      WHEN COALESCE(
        lr.total_reviews,
        0
      ) BETWEEN 10
      AND 49 THEN 'Moderate'
      WHEN COALESCE(
        lr.total_reviews,
        0
      ) BETWEEN 1
      AND 9 THEN 'New/Low Activity'
      ELSE 'No Reviews'
    END AS popularity_tier,
    CASE
      WHEN lr.days_since_last_review IS NULL THEN 'No Reviews'
      WHEN lr.days_since_last_review <= 30 THEN 'Very Active'
      WHEN lr.days_since_last_review BETWEEN 31
      AND 90 THEN 'Active'
      WHEN lr.days_since_last_review BETWEEN 91
      AND 180 THEN 'Moderately Active'
      WHEN lr.days_since_last_review > 180 THEN 'Inactive'
      ELSE 'Unknown'
    END AS activity_status,
    CASE
      WHEN l.price > 0
      AND COALESCE(
        lr.total_reviews,
        0
      ) > 0 THEN ROUND(
        (COALESCE(lr.positive_reviews, 0) * 100.0 / NULLIF(lr.total_reviews, 0)) * 0.7 + (LEAST(lr.total_reviews, 100) / 100.0 * 100) * 0.3,
        2
      )
      ELSE 0
    END AS listing_quality_score,
    TRUE AS is_current,
    l.dbt_loaded_at AS effective_date,
    NULL :: TIMESTAMP AS end_date,
    CURRENT_TIMESTAMP() AS dbt_updated_at
  FROM
    stg_listings l
    LEFT JOIN listing_reviews lr
    ON l.listing_id = lr.listing_id
)
SELECT
  *
FROM
  FINAL
