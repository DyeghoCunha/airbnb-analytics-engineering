-- Teste singular: valida distribuição de preços

-- Verifica se há uma quantidade anormal de preços com valores suspeitos
WITH price_analysis AS (
    SELECT
        COUNT(*) AS total_listings,
        COUNT(CASE WHEN price = 0 THEN 1 END) AS zero_price_count,
        COUNT(CASE WHEN price > 10000 THEN 1 END) AS extreme_high_price_count,
        AVG(price) AS avg_price,
        STDDEV(price) AS stddev_price
    FROM {{ ref('stg_listings') }}
)

SELECT *
FROM price_analysis
WHERE 
    -- Mais de 5% dos listings com preço zero é suspeito
    (zero_price_count * 100.0 / total_listings) > 5
    OR
    -- Mais de 1% com preços extremamente altos é suspeito
    (extreme_high_price_count * 100.0 / total_listings) > 1
    OR
    -- Desvio padrão muito alto indica problemas
    stddev_price > (avg_price * 2)