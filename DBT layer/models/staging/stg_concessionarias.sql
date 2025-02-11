{{ 
    config(
        schema='STAGING',
        materialized='table',
        tags=['stg_concessionarias', 'staging']
    ) 
}}

WITH source AS (
    SELECT
        id_concessionarias,
        TRIM(concessionaria) AS nome_concessionaria, 
        id_cidades,
        data_inclusao,
        COALESCE(data_atualizacao, data_inclusao) AS data_atualizacao 
    FROM {{ source('sources', 'CONCESSIONARIAS') }}
)

SELECT
    id_concessionarias,
    nome_concessionaria,
    id_cidades,
    data_inclusao,
    data_atualizacao
FROM source
