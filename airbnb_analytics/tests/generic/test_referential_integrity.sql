-- Teste de integridade referencial customizado
{% test referential_integrity(model, column_name, to, field) %}
WITH child AS (
    SELECT DISTINCT {{ column_name }} AS child_key
    FROM {{ model }}
    WHERE {{ column_name }} IS NOT NULL
),
parent AS (
    SELECT DISTINCT {{ field }} AS parent_key
    FROM {{ to }}
)
-- Encontra chaves órfãs (existem no child mas não no parent)
SELECT 
    c.child_key,
    'Orphan record: exists in child but not in parent' AS error_message
FROM child c
LEFT JOIN parent p ON c.child_key = p.parent_key
WHERE p.parent_key IS NULL

{% endtest %}