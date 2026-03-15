-- Teste singular: valida lógica de datas em reviews

-- Reviews devem ter datas lógicas (não futuras, não antes da criação do listing)
WITH review_date_validation AS (
    SELECT
        r.listing_id,
        r.review_date,
        l.listing_created_at,
        CASE 
            WHEN r.review_date > CURRENT_DATE() THEN 'Future review date'
            WHEN r.review_date < DATE(l.listing_created_at) THEN 'Review before listing creation'
            ELSE NULL
        END AS validation_error
    FROM {{ ref('stg_reviews') }} r
    INNER JOIN {{ ref('stg_listings') }} l ON r.listing_id = l.listing_id
)

SELECT *
FROM review_date_validation
WHERE validation_error IS NOT NULL