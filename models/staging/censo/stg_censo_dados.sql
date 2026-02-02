{{ config(
    materialized='view',
    tags=['censo', 'staging']
) }}

/* Padrão de CTEs (Common Table Expressions):
    1. import_ctes: Traz as tabelas fontes.
    2. renamed: Renomeia, tipa e limpa os dados.
    3. final: Seleciona o resultado.
*/

WITH source AS (
    SELECT * FROM {{ source('censo', 'raw_censo_2024') }}
),

SELECT * FROM source