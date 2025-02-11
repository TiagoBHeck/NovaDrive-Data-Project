{{ 
    config(
        schema='STAGING',
        materialized='table',
        tags=['stg_veiculos', 'staging']
    ) 
}}

WITH source AS (
    SELECT
        id_veiculos,
        nome,
        tipo,
        valor::DECIMAL(10,2) AS valor,
        COALESCE(data_atualizacao, CURRENT_TIMESTAMP()) AS data_atualizacao,
        data_inclusao
    FROM {{ source('sources', 'VEICULOS') }}
)

SELECT
    id_veiculos,
    nome,
    tipo,
    valor,
    data_atualizacao,
    data_inclusao
FROM source
