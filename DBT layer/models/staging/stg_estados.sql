{{ 
    config(
        schema='STAGING',
        materialized='table',
        tags=['stg_estados', 'staging']
    ) 
}}

WITH source AS (
    SELECT
        id_estados,
        UPPER(estado) AS estado,
        UPPER(sigla) AS sigla,
        data_inclusao,
        data_atualizacao
    FROM {{ source('sources', 'ESTADOS') }}
)

SELECT
    id_estados,
    estado,
    sigla,
    data_inclusao,
    data_atualizacao
FROM source