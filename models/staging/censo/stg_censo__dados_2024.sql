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

renamed AS (
    SELECT
        -- Time and Geography
        CAST(NU_ANO_CENSO AS INT) AS census_year,
        CAST(CO_REGIAO AS INT) AS region_id,
        NO_REGIAO AS region_name,
        CAST(CO_UF AS INT) AS state_id,
        SG_UF AS state_code,
        NO_UF AS state_name,
        CAST(CO_MUNICIPIO AS INT) AS city_id,
        NO_MUNICIPIO AS city_name,
        CAST(CO_CEP AS STRING) AS zip_code,

        -- School Identification
        CAST(CO_ENTIDADE AS INT) AS school_id,
        NO_ENTIDADE AS school_name,
        CAST(TP_DEPENDENCIA AS INT) AS dependency_type_id,
        CASE WHEN CAST(TP_DEPENDENCIA AS INT) = 4 THEN 'Privada' ELSE 'Pública' END AS school_type,
        CAST(TP_SITUACAO_FUNCIONAMENTO AS INT) AS operational_status_id,
        
        -- Location and Contact
        CAST(TP_LOCALIZACAO AS INT) AS location_type_id,
        DS_ENDERECO AS address,
        NU_ENDERECO AS address_number,
        DS_COMPLEMENTO AS address_complement,
        NO_BAIRRO AS neighborhood,
        NU_DDD AS phone_area_code,
        NU_TELEFONE AS phone_number,

        -- Infrastructure (Flags to Boolean)
        CAST(IN_AGUA_POTAVEL AS BOOLEAN) AS has_potable_water,
        CAST(IN_ENERGIA_REDE_PUBLICA AS BOOLEAN) AS has_public_grid_energy,
        CAST(IN_ESGOTO_REDE_PUBLICA AS BOOLEAN) AS has_public_sewage,
        CAST(IN_LIXO_SERVICO_COLETA AS BOOLEAN) AS has_trash_collection,
        CAST(IN_INTERNET AS BOOLEAN) AS has_internet,
        CAST(IN_BANDA_LARGA AS BOOLEAN) AS has_broadband,
        CAST(IN_BIBLIOTECA AS BOOLEAN) AS has_library,
        CAST(IN_COZINHA AS BOOLEAN) AS has_kitchen,
        CAST(IN_LABORATORIO_INFORMATICA AS BOOLEAN) AS has_it_lab,
        CAST(IN_LABORATORIO_CIENCIAS AS BOOLEAN) AS has_science_lab,
        CAST(IN_QUADRA_ESPORTES AS BOOLEAN) AS has_sports_court,
        CAST(TP_AEE AS BOOLEAN) AS has_special_education_services,

        -- Counts (Rooms and Equipment)
        CAST(QT_SALAS_UTILIZADAS AS INT) AS total_classrooms_in_use,
        CAST(QT_DESKTOP_ALUNO AS INT) AS qty_student_desktops,
        CAST(QT_COMP_PORTATIL_ALUNO AS INT) AS qty_student_laptops,
        CAST(QT_TABLET_ALUNO AS INT) AS qty_student_tablets,

        -- Enrollment Counts
        CAST(QT_MAT_BAS AS INT) AS total_enrollments_basic,
        CAST(QT_MAT_INF AS INT) AS total_enrollments_infant,
        CAST(QT_MAT_FUND AS INT) AS total_enrollments_elementary,
        CAST(QT_MAT_MED AS INT) AS total_enrollments_high_school,
        CAST(QT_MAT_PROF AS INT) AS total_enrollments_vocational,
        CAST(QT_MAT_EJA AS INT) AS total_enrollments_eja,
        CAST(QT_MAT_ESP AS INT) AS total_enrollments_special_ed,

        -- Staff and Classroom Counts
        CAST(QT_DOC_BAS AS INT) AS total_teachers_basic,
        CAST(QT_TUR_BAS AS INT) AS total_classes_basic

    FROM source
)

SELECT * FROM renamed
