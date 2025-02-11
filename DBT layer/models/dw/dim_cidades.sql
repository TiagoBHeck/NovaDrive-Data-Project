{{ 
    config(
        schema='DW',
        materialized='table',
        unique_key='sk_cidade',
        tags=['dim_cidades', 'dw']
    )
}}

WITH cidades AS (
    SELECT
        id_cidades AS cidade_id,
        nome_cidade,
        id_estados AS estado_id,
        data_inclusao,
        data_atualizacao
    FROM {{ ref('stg_cidades') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['cidade_id']) }} AS sk_cidade,
    cidade_id,
    nome_cidade,
    estado_id,
    data_inclusao,
    data_atualizacao
FROM
cidades