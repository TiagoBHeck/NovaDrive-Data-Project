{{ 
    config(
        schema='STAGING',
        materialized='table',
        tags=['stg_clientes', 'staging']
    ) 
}}

WITH source AS (
    SELECT
        id_cidades,
        INITCAP(cidade) AS nome_cidade, 
        id_estados,
        data_inclusao,
        COALESCE(data_atualizacao, data_inclusao) AS data_atualizacao 
    FROM {{ source('sources', 'CIDADES') }}
)

SELECT
    id_cidades,
    nome_cidade,
    id_estados,
    data_inclusao,
    data_atualizacao
FROM source