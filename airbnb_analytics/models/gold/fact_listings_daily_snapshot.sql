{{
    config(
        materialized='incremental',
        unique_key=['listing_id', 'snapshot_date'],
        tags=['gold', 'fact', 'snapshot']
    )
}}


WITH dim_listing AS (
    SELECT * FROM {{ ref('dim_listing') }}
),

dim_host AS (
    SELECT * FROM {{ ref('dim_host') }}
),

dim_date AS (
    SELECT * FROM {{ ref('dim_date') }}
    WHERE date <= CURRENT_DATE()
    
    {% if is_incremental() %}
    AND date > (SELECT MAX(snapshot_date) FROM {{ this }})
    {% endif %}
),

listings_daily AS (
    SELECT
        dl.listing_key,
        dl.listing_id,
        dl.host_id,
        dd.date_key,
        dd.date AS snapshot_date,

        dl.price,
        dl.minimum_nights,
        dl.is_bookable,
        dl.room_type,
        dl.price_category,
        dl.total_reviews,
        dl.positive_review_rate,
        dl.listing_quality_score,
        dl.popularity_tier,
        dl.activity_status
        
    FROM dim_listing dl
    CROSS JOIN dim_date dd

    WHERE dd.date >= DATE(dl.listing_created_at)
),

final AS (
    SELECT

        {{ dbt_utils.generate_surrogate_key(['ld.listing_id', 'ld.snapshot_date']) }} AS snapshot_key,

        ld.listing_key,
        dh.host_key,
        ld.date_key,

        ld.listing_id,
        ld.snapshot_date,

        ld.price AS daily_price,
        ld.minimum_nights,
        ld.is_bookable,

        ld.room_type,
        ld.price_category,
        ld.popularity_tier,
        ld.activity_status,

        ld.total_reviews AS reviews_to_date,
        ld.positive_review_rate,
        ld.listing_quality_score,

        CASE WHEN ld.is_bookable THEN 1 ELSE 0 END AS is_bookable_flag,

        CURRENT_TIMESTAMP() AS dbt_updated_at
        
    FROM listings_daily ld
    INNER JOIN dim_host dh ON ld.host_id = dh.host_id
)

SELECT * FROM final