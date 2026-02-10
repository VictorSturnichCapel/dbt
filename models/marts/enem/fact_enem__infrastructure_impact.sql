{{ config(
    materialized='table',
    tags=['enem', 'marts']
) }}

WITH performance AS (
    SELECT * FROM {{ ref('int_enem__school_performance_aggregated') }}
),

infra AS (
    SELECT * FROM {{ ref('int_censo__school_infrastructure_score') }}
)

SELECT
    p.school_id,
    i.school_name,
    p.exam_year,
    p.school_state,
    p.school_city_name,
    p.total_students,
    p.school_type,
    p.avg_score_natural_sciences,
    p.avg_score_humanities,
    p.avg_score_languages,
    p.avg_score_math,
    p.avg_score_essay,
    p.school_general_average,
    p.general_rank,
    p.general_state_rank,
    p.general_city_rank,
    p.general_type_rank,
    p.general_type_state_rank,
    p.general_type_city_rank,
    i.tech_score,
    i.facility_score,
    i.students_per_classroom,
    -- KPI de eficiência: Nota por dispositivo disponível
    SAFE_DIVIDE(p.school_general_average, i.devices_per_student) AS score_per_device_ratio,
    -- Ranking de infraestrutura dentro da mesma cidade
    RANK() OVER(PARTITION BY p.school_city_name ORDER BY i.tech_score DESC) AS tech_rank_city
FROM performance p
LEFT JOIN infra i ON p.school_id = i.school_id