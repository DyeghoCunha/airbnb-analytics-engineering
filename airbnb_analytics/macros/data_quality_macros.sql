-- Macro para calcular completeness de uma coluna

{% macro calculate_completeness(
  model,
  column_name
) %}
SELECT
  '{{ model }}' AS model_name,
  '{{ column_name }}' AS column_name,
  COUNT(*) AS total_rows,
  COUNT(
    {{ column_name }}
  ) AS non_null_rows,
  ROUND(COUNT({{ column_name }}) * 100.0 / COUNT(*), 2) AS completeness_pct
FROM
  {{ ref(model) }}
{% endmacro %}

{% macro detect_outliers(model, column_name) %}
WITH stats AS (
    SELECT 
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY {{ column_name }}) AS q1,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY {{ column_name }}) AS q3
    FROM {{ ref(model) }}
),
iqr_calc AS (
    SELECT 
        q1,
        q3,
        (q3 - q1) AS iqr,
        (q1 - 1.5 * (q3 - q1)) AS lower_bound,
        (q3 + 1.5 * (q3 - q1)) AS upper_bound
    FROM stats
)
SELECT 
    m.*,
    CASE 
        WHEN m.{{ column_name }} < i.lower_bound 
          OR m.{{ column_name }} > i.upper_bound THEN TRUE 
        ELSE FALSE 
    END AS is_outlier
FROM {{ ref(model) }} AS m
CROSS JOIN iqr_calc AS i
{% endmacro %}

{% macro generate_surrogate_key(columns) %}
    MD5(CONCAT_WS('||', {{ columns | join(', ') }}))
{% endmacro %}