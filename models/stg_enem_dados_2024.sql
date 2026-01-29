{{ config(
    materialized='view',
    tags=['enem', 'staging']
) }}

/* Padrão de CTEs (Common Table Expressions):
    1. import_ctes: Traz as tabelas fontes.
    2. renamed: Renomeia, tipa e limpa os dados.
    3. final: Seleciona o resultado.
*/

WITH source AS (
    SELECT * FROM {{ source('raw_enem', 'stg_enem_2024') }}
),

renamed AS (
    SELECT
        -- Identificadores
        CAST(NU_SEQUENCIAL AS STRING) AS student_id,
        CAST(NU_ANO AS INT64) AS exam_year,
        
        -- Escola e Localização (School & Location)
        CAST(CO_ESCOLA AS STRING) AS school_id,
        CAST(CO_MUNICIPIO_ESC AS STRING) AS school_city_id,
        INITCAP(NO_MUNICIPIO_ESC) AS school_city_name,
        SG_UF_ESC AS school_state,
        TP_DEPENDENCIA_ADM_ESC AS school_admin_type_id, -- Sugere-se criar tabela dimensão para decodificar
        TP_LOCALIZACAO_ESC AS school_location_type_id,
        TP_SIT_FUNC_ESC AS school_status_id,

        -- Local de Prova (Exam Location)
        CAST(CO_MUNICIPIO_PROVA AS STRING) AS exam_city_id,
        INITCAP(NO_MUNICIPIO_PROVA) AS exam_city_name,
        SG_UF_PROVA AS exam_state,

        -- Presença (Presence) - Bool ou Int
        TP_PRESENCA_CN AS is_present_natural_sciences,
        TP_PRESENCA_CH AS is_present_humanities,
        TP_PRESENCA_LC AS is_present_languages,
        TP_PRESENCA_MT AS is_present_math,

        -- Códigos da Prova (Exam Version IDs)
        CO_PROVA_CN AS natural_sciences_exam_id,
        CO_PROVA_CH AS humanities_exam_id,
        CO_PROVA_LC AS languages_exam_id,
        CO_PROVA_MT AS math_exam_id,

        -- Notas (Scores) - Tratamento de Tipagem para Float
        -- Utiliza SAFE_CAST para evitar erros se vier string vazia ou suja
        SAFE_CAST(NU_NOTA_CN AS FLOAT64) AS natural_sciences_score,
        SAFE_CAST(NU_NOTA_CH AS FLOAT64) AS humanities_score,
        SAFE_CAST(NU_NOTA_LC AS FLOAT64) AS languages_score,
        SAFE_CAST(NU_NOTA_MT AS FLOAT64) AS math_score,
        
        -- Notas da Redação (Essay Scores)
        TP_STATUS_REDACAO AS essay_status_id,
        SAFE_CAST(NU_NOTA_COMP1 AS INT64) AS essay_competence_1_score,
        SAFE_CAST(NU_NOTA_COMP2 AS INT64) AS essay_competence_2_score,
        SAFE_CAST(NU_NOTA_COMP3 AS INT64) AS essay_competence_3_score,
        SAFE_CAST(NU_NOTA_COMP4 AS INT64) AS essay_competence_4_score,
        SAFE_CAST(NU_NOTA_COMP5 AS INT64) AS essay_competence_5_score,
        SAFE_CAST(NU_NOTA_REDACAO AS INT64) AS essay_total_score,

        -- Metadados de Respostas (Opcional manter na staging se for usar para análise de itens)
        TP_LINGUA AS foreign_language_type, -- 0=Inglês, 1=Espanhol
        TX_RESPOSTAS_CN AS natural_sciences_answers,
        TX_RESPOSTAS_CH AS humanities_answers,
        TX_RESPOSTAS_LC AS languages_answers,
        TX_RESPOSTAS_MT AS math_answers,
        TX_GABARITO_CN AS natural_sciences_key,
        TX_GABARITO_CH AS humanities_key,
        TX_GABARITO_LC AS languages_key,
        TX_GABARITO_MT AS math_key

    FROM source
)


SELECT * FROM renamed