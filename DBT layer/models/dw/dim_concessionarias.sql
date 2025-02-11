{{ 
    config(
        schema='DW',
        materialized='table',
        unique_key='sk_concessionaria',
        tags=['dim_concessionarias', 'dw']
    )
}}

WITH concessionarias AS (
    SELECT
        id_concessionarias AS concessionaria_id,
        nome_concessionaria,
        id_cidades AS cidade_id,
        data_inclusao,
        data_atualizacao
    FROM {{ ref('stg_concessionarias') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['concessionaria_id']) }} AS sk_concessionaria,
    concessionaria_id,
    nome_concessionaria,
    cidade_id,
    data_inclusao,
    data_atualizacao
FROM concessionarias