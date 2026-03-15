-- Teste singular: consistência entre hosts e listings
-- Todos os hosts devem ter pelo menos 1 listing
WITH hosts_without_listings AS (
    SELECT 
        h.host_id,
        h.host_name,
        COUNT(l.listing_id) AS listing_count
    FROM {{ ref('stg_hosts') }} h
    LEFT JOIN {{ ref('stg_listings') }} l ON h.host_id = l.host_id
    GROUP BY h.host_id, h.host_name
    HAVING COUNT(l.listing_id) = 0
)

SELECT *
FROM hosts_without_listings