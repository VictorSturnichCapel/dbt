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
        COUNT(student_id) AS total_students,
        
        -- Médias de Notas (SAFE_AVG para evitar divisões por zero ou nulos)
        ROUND(AVG(natural_sciences_score), 2) AS avg_score_natural_sciences,
        ROUND(AVG(humanities_score), 2) AS avg_score_humanities,
        ROUND(AVG(languages_score), 2) AS avg_score_languages,
        ROUND(AVG(math_score), 2) AS avg_score_math,
        ROUND(AVG(essay_total_score), 2) AS avg_score_essay,

    FROM staging_enem
    -- Needs to go on both days and do not zero essay
    WHERE 1=1
    AND (is_present_natural_sciences + is_present_humanities + is_present_languages + is_present_math) = 4
    AND essay_total_score > 0
    GROUP BY 1, 2, 3
    -- Having more than 9 students per school
    HAVING COUNT(school_id) > 9
),

final AS (
    SELECT
        *,
        RANK() OVER(
            PARTITION BY exam_year 
            ORDER BY (
                COALESCE(avg_score_natural_sciences, 0) + 
                COALESCE(avg_score_humanities, 0) + 
                COALESCE(avg_score_languages, 0) + 
                COALESCE(avg_score_math, 0) + 
                COALESCE(avg_score_essay, 0)
            ) / 5 DESC
        ) AS general_rank,
        RANK() OVER(
            PARTITION BY exam_year, school_state
            ORDER BY (
                COALESCE(avg_score_natural_sciences, 0) + 
                COALESCE(avg_score_humanities, 0) + 
                COALESCE(avg_score_languages, 0) + 
                COALESCE(avg_score_math, 0) + 
                COALESCE(avg_score_essay, 0)
            ) / 5 DESC
        ) AS general_state_rank,
        -- Nota Geral da Escola (Média das médias)
        ROUND((avg_score_natural_sciences + avg_score_humanities + avg_score_languages + avg_score_math + avg_score_essay) / 5, 2) AS school_general_average
    FROM school_metrics
)

SELECT * FROM final