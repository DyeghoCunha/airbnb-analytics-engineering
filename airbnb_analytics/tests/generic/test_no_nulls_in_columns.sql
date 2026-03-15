-- Teste genérico customizado: verifica se há nulls em múltiplas colunas
{% test no_nulls_in_columns(model, column_names) %}

WITH validation AS (
    SELECT
        {% for column_name in column_names %}
        SUM(CASE WHEN {{ column_name }} IS NULL THEN 1 ELSE 0 END) AS {{ column_name }}_null_count
        {% if not loop.last %},{% endif %}
        {% endfor %}
    FROM {{ model }}
)

SELECT *
FROM validation
WHERE 
    {% for column_name in column_names %}
    {{ column_name }}_null_count > 0
    {% if not loop.last %}OR{% endif %}
    {% endfor %}

{% endtest %}