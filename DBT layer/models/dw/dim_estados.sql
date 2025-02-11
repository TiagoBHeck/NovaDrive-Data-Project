{{ 
    config(
        schema='DW',
        materialized='table',
        unique_key='sk_estado',
        tags=['dim_estados', 'dw']
    )
}}

WITH estados AS (
    SELECT
        id_estados AS estado_id,
        estado AS nome_estado,
        sigla,
        data_inclusao,
        data_atualizacao
    FROM {{ ref('stg_estados') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['estado_id']) }} AS sk_estado,
    estado_id,
    nome_estado,
    sigla,
    data_inclusao,
    data_atualizacao
FROM estados