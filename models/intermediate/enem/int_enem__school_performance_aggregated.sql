{{ config(
    materialized='table',
    tags=['enem', 'intermediate']
) }}

WITH staging_enem AS (
    SELECT * FROM {{ source('enem_staging', 'stg_enem__dados_2024') }}
    WHERE school_id IS NOT NULL -- Foco apenas em alunos vinculados a escolas
),

school_metrics AS (
    SELECT
        school_id,
        school_state,
        exam_year,
        
        -- Contagem de Alunos
        COUNT(student_id) AS total_students_registered,
        
        -- Médias de Notas (SAFE_AVG para evitar divisões por zero ou nulos)
        AVG(natural_sciences_score) AS avg_score_natural_sciences,
        AVG(humanities_score) AS avg_score_humanities,
        AVG(languages_score) AS avg_score_languages,
        AVG(math_score) AS avg_score_math,
        AVG(essay_total_score) AS avg_score_essay,
        
        -- Cálculo de Presença (Taxa de comparecimento)
        -- Usando a média de um booleano/int para obter o %
        AVG(CAST(is_present_natural_sciences AS INT64)) AS attendance_rate_day_1,
        AVG(CAST(is_present_math AS INT64)) AS attendance_rate_day_2

    FROM staging_enem
    GROUP BY 1, 2, 3
),

final AS (
    SELECT
        *,
        -- Nota Geral da Escola (Média das médias)
        (avg_score_natural_sciences + avg_score_humanities + avg_score_languages + avg_score_math + avg_score_essay) / 5 AS school_general_average
    FROM school_metrics
)

SELECT * FROM final