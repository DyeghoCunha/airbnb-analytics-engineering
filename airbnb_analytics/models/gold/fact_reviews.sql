{{
    config(
        materialized='incremental',
        unique_key='review_key',
        tags=['gold', 'fact']
    )
}}

WITH stg_reviews AS (
    SELECT * FROM {{ ref('stg_reviews') }}
    {% if is_incremental() %}
    WHERE review_date > (SELECT MAX(review_date) FROM {{ this }})
    {% endif %}
),

dim_listing AS (
    SELECT 
        listing_key,
        listing_id,
        host_id
    FROM {{ ref('dim_listing') }}
),

dim_host AS (
    SELECT
        host_key,
        host_id
    FROM {{ ref('dim_host') }}
),

dim_date AS (
    SELECT
        date_key,
        date
    FROM {{ ref('dim_date') }}
),

final AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['r.listing_id', 'r.review_date', 'r.reviewer_name']) }} AS review_key,
        
        dl.listing_key,
        dh.host_key,
        dd.date_key,
        
        r.listing_id,
        r.review_date,
        
        r.reviewer_name,
        
        1 AS review_count,  -- Para COUNT em agregações
        r.review_length,
        r.word_count,
        
        -- Flags para agregações condicionais
        CASE WHEN r.sentiment_category = 'Positive' THEN 1 ELSE 0 END AS is_positive,
        CASE WHEN r.sentiment_category = 'Negative' THEN 1 ELSE 0 END AS is_negative,
        CASE WHEN r.sentiment_category = 'Neutral' THEN 1 ELSE 0 END AS is_neutral,
        
        CASE WHEN r.mentions_clean THEN 1 ELSE 0 END AS mentions_clean_flag,
        CASE WHEN r.mentions_location THEN 1 ELSE 0 END AS mentions_location_flag,
        CASE WHEN r.mentions_host THEN 1 ELSE 0 END AS mentions_host_flag,
        
        -- Atributos não-aditivos (para contexto)
        r.sentiment_category,
        r.review_length_category,
        r.review_comments,  -- Pode ser útil para drill-down
        
        -- Métricas calculadas
        CASE 
            WHEN r.word_count > 0 THEN ROUND(r.review_length * 1.0 / r.word_count, 2)
            ELSE 0
        END AS avg_word_length,
        
        -- Metadata
        r.dbt_loaded_at,
        CURRENT_TIMESTAMP() AS dbt_updated_at
        
    FROM stg_reviews r
    INNER JOIN dim_listing dl ON r.listing_id = dl.listing_id
    INNER JOIN dim_host dh ON dl.host_id = dh.host_id
    LEFT JOIN dim_date dd ON r.review_date = dd.date
)

SELECT * FROM final