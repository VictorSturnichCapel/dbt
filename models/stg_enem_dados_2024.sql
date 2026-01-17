-- Este comando diz ao dbt para criar uma TABELA no BigQuery
{{ config(materialized='table') }}

SELECT 
    *
FROM {{ source('raw_enem', 'stg_enem_2024') }}