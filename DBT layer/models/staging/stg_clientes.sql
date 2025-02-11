{{ 
    config(
        schema='STAGING',
        materialized='table',
        tags=['stg_clientes', 'staging']
    ) 
}}

WITH source AS (
    SELECT
        id_clientes,
        INITCAP(cliente) AS cliente,
        TRIM(endereco) AS endereco,
        id_concessionarias,
        data_inclusao,
        COALESCE(data_atualizacao, data_inclusao) AS data_atualizacao
    FROM {{ source('sources', 'CLIENTES') }}
)

SELECT
    id_clientes,
    cliente,
    endereco,
    id_concessionarias,
    data_inclusao,
    data_atualizacao
FROM source