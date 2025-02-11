{{ 
    config(
        schema='DW',
        materialized='table',
        unique_key='sk_vendedor',
        tags=['dim_vendedores', 'dw']
    )
}}


WITH vendedores AS (
    SELECT
        id_vendedores AS vendedor_id,
        nome_vendedor,
        id_concessionarias AS concessionaria_id,
        data_inclusao,
        data_atualizacao
    FROM {{ ref('stg_vendedores') }}
)

SELECT 
    {{ dbt_utils.generate_surrogate_key(['vendedor_id']) }} AS sk_vendedor,
    vendedor_id,
    nome_vendedor,
    concessionaria_id,
    data_inclusao,
    data_atualizacao
FROM vendedores