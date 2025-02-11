{{ 
    config(
        schema='STAGING',
        materialized='table',
        tags=['stg_vendedores', 'staging']
    ) 
}}

WITH source AS (
    SELECT
        id_vendedores,
        INITCAP(nome) AS nome_vendedor, 
        id_concessionarias,
        data_inclusao,
        COALESCE(data_atualizacao, data_inclusao) AS data_atualizacao 
    FROM {{ source('sources', 'VENDEDORES') }}
)

SELECT
    id_vendedores,
    nome_vendedor,
    id_concessionarias,
    data_inclusao,
    data_atualizacao
FROM source
