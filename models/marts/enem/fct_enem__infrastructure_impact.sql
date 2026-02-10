{{ config(materialized='table') }}

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
    p.school_type,
    p.school_general_average,
    i.tech_score,
    i.facility_score,
    i.students_per_classroom,
    -- KPI de eficiência: Nota por dispositivo disponível
    SAFE_DIVIDE(p.school_general_average, i.devices_per_student) AS score_per_device_ratio,
    -- Ranking de infraestrutura dentro da mesma cidade
    RANK() OVER(PARTITION BY p.school_city_name ORDER BY i.tech_score DESC) AS tech_rank_city
FROM performance p
LEFT JOIN infra i ON p.school_id = i.school_id