{{
    config(
        materialized='table',
        tags=['gold', 'dimension']
    )
}}


WITH date_spine AS (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2009-06-20' as date)",
        end_date="cast('2026-02-20' as date)"
    ) }}
),
final AS (
    SELECT
        -- Surrogate Key
        {{ dbt_utils.generate_surrogate_key(['date_day']) }} AS date_key,
        
        -- Natural Key
        date_day AS date,
        
        -- Componentes de data
        YEAR(date_day) AS year,
        QUARTER(date_day) AS quarter,
        MONTH(date_day) AS month,
        DAYOFMONTH(date_day) AS day,
        DAYOFWEEK(date_day) AS day_of_week,
        DAYOFYEAR(date_day) AS day_of_year,
        WEEKOFYEAR(date_day) AS week_of_year,
        
        -- Nomes formatados
        DATE_FORMAT(date_day, 'MMMM') AS month_name,
        DATE_FORMAT(date_day, 'EEEE') AS day_name,
        CONCAT('Q', QUARTER(date_day), ' ', YEAR(date_day)) AS quarter_name,
        
        -- Abreviações
        DATE_FORMAT(date_day, 'MMM') AS month_name_short,
        DATE_FORMAT(date_day, 'EEE') AS day_name_short,
        
        -- Flags úteis para análise
        CASE WHEN DAYOFWEEK(date_day) IN (1, 7) THEN TRUE ELSE FALSE END AS is_weekend,
        CASE WHEN DAYOFWEEK(date_day) BETWEEN 2 AND 6 THEN TRUE ELSE FALSE END AS is_weekday,
        
        -- Identificar primeiro/último dia do mês
        CASE WHEN DAYOFMONTH(date_day) = 1 THEN TRUE ELSE FALSE END AS is_first_day_of_month,
        CASE WHEN DAYOFMONTH(date_day) = DAYOFMONTH(LAST_DAY(date_day)) THEN TRUE ELSE FALSE END AS is_last_day_of_month,
        
        -- Fiscal Year (exemplo: ano fiscal começa em Julho)
        CASE 
            WHEN MONTH(date_day) >= 7 THEN YEAR(date_day) + 1
            ELSE YEAR(date_day)
        END AS fiscal_year,
        
        -- Períodos relativos (útil para filtros de BI)
        CASE 
            WHEN date_day = CURRENT_DATE() THEN 'Today'
            WHEN date_day = CURRENT_DATE() - INTERVAL 1 DAY THEN 'Yesterday'
            WHEN date_day BETWEEN CURRENT_DATE() - INTERVAL 7 DAYS AND CURRENT_DATE() - INTERVAL 1 DAY THEN 'Last 7 Days'
            WHEN date_day BETWEEN CURRENT_DATE() - INTERVAL 30 DAYS AND CURRENT_DATE() - INTERVAL 1 DAY THEN 'Last 30 Days'
            WHEN YEAR(date_day) = YEAR(CURRENT_DATE()) AND MONTH(date_day) = MONTH(CURRENT_DATE()) THEN 'Current Month'
            WHEN YEAR(date_day) = YEAR(CURRENT_DATE()) THEN 'Current Year'
            ELSE 'Historical'
        END AS relative_period,
        
        -- Metadata
        CURRENT_TIMESTAMP() AS dbt_updated_at
        
    FROM date_spine
)

SELECT * FROM final