{{
    config(
        materialized='table',
        tags=['gold', 'dimension']
    )
}}

-- Como você não tem dados de localização explícitos nas raw tables,
-- vamos criar uma dimensão simplificada baseada em patterns de nomes

WITH listings AS (
    SELECT DISTINCT
        listing_name,
        listing_id
    FROM {{ ref('stg_listings') }}
),

location_extracted AS (
    SELECT
        listing_id,
        listing_name,
     CASE 
    WHEN LOWER(listing_name) LIKE '%mitte%' THEN 'Mitte'
    WHEN LOWER(listing_name) LIKE '%prenzlauer berg%' OR LOWER(listing_name) LIKE '%p-berg%' OR LOWER(listing_name) LIKE '%prenzlberg%' THEN 'Prenzlauer Berg'
    WHEN LOWER(listing_name) LIKE '%kreuzberg%' OR LOWER(listing_name) LIKE '%xberg%' OR LOWER(listing_name) LIKE '%x-berg%' THEN 'Kreuzberg'
    WHEN LOWER(listing_name) LIKE '%neukölln%' OR LOWER(listing_name) LIKE '%neukolln%' THEN 'Neukölln'
    WHEN LOWER(listing_name) LIKE '%friedrichshain%' OR LOWER(listing_name) LIKE '%fhain%' OR LOWER(listing_name) LIKE '%f-hain%' THEN 'Friedrichshain'
    WHEN LOWER(listing_name) LIKE '%charlottenburg%' THEN 'Charlottenburg'
    WHEN LOWER(listing_name) LIKE '%schöneberg%' OR LOWER(listing_name) LIKE '%schoneberg%' THEN 'Schöneberg'
    WHEN LOWER(listing_name) LIKE '%wedding%' THEN 'Wedding'
    WHEN LOWER(listing_name) LIKE '%moabit%' THEN 'Moabit'
    WHEN LOWER(listing_name) LIKE '%pankow%' THEN 'Pankow'
    
    WHEN LOWER(listing_name) LIKE '%alexanderplatz%' OR LOWER(listing_name) LIKE '%potsdamer platz%' OR LOWER(listing_name) LIKE '%checkpoint charlie%' THEN 'Major Landmark'
    WHEN LOWER(listing_name) LIKE '%kudamm%' OR LOWER(listing_name) LIKE '%kurfürstendamm%' THEN 'City West / Shopping'
    
    WHEN LOWER(listing_name) LIKE '%city center%' OR LOWER(listing_name) LIKE '%central%' OR LOWER(listing_name) LIKE '%heart of%' THEN 'Central Area'
    
    ELSE 'Other / Residential'
END AS area_type,
        
        'Unknown' AS city,
        'Unknown' AS neighborhood
        
    FROM listings
),

location_stats AS (
    SELECT
        area_type,
        COUNT(DISTINCT listing_id) AS total_listings_in_area
    FROM location_extracted
    GROUP BY area_type
),

final AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['le.listing_id']) }} AS location_key,
        le.listing_id,
        le.area_type,
        le.city,
        le.neighborhood,
        ls.total_listings_in_area,
        CASE
            WHEN ls.total_listings_in_area > 100 THEN 'High Density'
            WHEN ls.total_listings_in_area BETWEEN 50 AND 100 THEN 'Medium Density'
            WHEN ls.total_listings_in_area < 50 THEN 'Low Density'
            ELSE 'Unknown'
        END AS area_density,
        
        CURRENT_TIMESTAMP() AS dbt_updated_at
        
    FROM location_extracted le
    LEFT JOIN location_stats ls ON le.area_type = ls.area_type
)

SELECT * FROM final