{{ config(
    materialized='table',
    partition_by={
      "field": "census_year",
      "data_type": "int64",
      "range": {
        "start": 2020,
        "end": 2030,
        "interval": 1
      }
    },
    cluster_by=['state_code', 'school_id'],
    tags=['censo', 'intermediate']
) }}

WITH staging AS (
    SELECT * FROM {{ source('censo_staging', 'stg_censo__dados_2024') }}
),

calculated AS (
    SELECT
        school_id,
        school_name,
        -- Criando um score de tecnologia (0 a 3)
        (CAST(has_internet AS INT) + CAST(has_broadband AS INT) + CAST(has_it_lab AS INT)) AS tech_score,
        -- Criando um score de instalações (0 a 3)
        (CAST(has_library AS INT) + CAST(has_science_lab AS INT) + CAST(has_sports_court AS INT)) AS facility_score,
        -- Densidade de alunos por sala
        SAFE_DIVIDE(total_enrollments_basic, total_classrooms_in_use) AS students_per_classroom,
        -- Computadores por aluno
        SAFE_DIVIDE((qty_student_desktops + qty_student_laptops + qty_student_tablets), total_enrollments_basic) AS devices_per_student
    FROM staging
)

SELECT * FROM calculated