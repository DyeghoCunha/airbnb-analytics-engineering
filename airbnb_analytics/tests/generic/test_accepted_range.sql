-- Teste para validar se valores estão dentro de um range aceitável
{% test accepted_range(model, column_name, min_value=None, max_value=None) %}

SELECT *
FROM {{ model }}
WHERE {{ column_name }} IS NOT NULL
  {% if min_value is not none %}
  AND {{ column_name }} < {{ min_value }}
  {% endif %}
  {% if max_value is not none %}
  AND {{ column_name }} > {{ max_value }}
  {% endif %}

{% endtest %}