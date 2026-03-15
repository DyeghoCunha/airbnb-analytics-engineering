-- Macros auxiliares para testes

-- Macro para calcular percentual de completeness
{% macro get_column_completeness(model, column_name) %}
    SELECT 
        '{{ column_name }}' AS column_name,
        COUNT(*) AS total_rows,
        COUNT({{ column_name }}) AS non_null_rows,
        ROUND(COUNT({{ column_name }}) * 100.0 / COUNT(*), 2) AS completeness_pct
    FROM {{ model }}
{% endmacro %}

-- Macro para gerar relatório de qualidade de dados
{% macro generate_quality_report(model) %}
    {% set columns_query %}
        SELECT column_name 
        FROM information_schema.columns
        WHERE table_name = '{{ model.name }}'
        AND table_schema = '{{ model.schema }}'
    {% endset %}
    
    {% set results = run_query(columns_query) %}
    
    {% if execute %}
        {% set column_names = results.columns[0].values() %}
        
        SELECT 
            '{{ model.name }}' AS model_name,
            {% for column_name in column_names %}
            {{ get_column_completeness(model, column_name) }}
            {% if not loop.last %}UNION ALL{% endif %}
            {% endfor %}
    {% endif %}
{% endmacro %}

-- Macro para logging de execução
{% macro log_run_metadata(model_name, rows_affected) %}
    {% set query %}
        INSERT INTO {{ target.schema }}.dbt_run_log (
            model_name,
            run_timestamp,
            rows_affected,
            target_name
        )
        VALUES (
            '{{ model_name }}',
            CURRENT_TIMESTAMP(),
            {{ rows_affected }},
            '{{ target.name }}'
        )
    {% endset %}
    
    {% do run_query(query) %}
{% endmacro %}