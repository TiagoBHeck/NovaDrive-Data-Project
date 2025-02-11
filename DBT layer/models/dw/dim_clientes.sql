{{ 
    config(
        schema='DW',
        materialized='table',
        unique_key='sk_cliente',
        tags=['dim_clientes', 'dw']
    )
}}

WITH clientes AS (
    SELECT
        id_clientes AS cliente_id,
        cliente AS nome_cliente,
        endereco,
        id_concessionarias AS concessionaria_id,
        data_inclusao,
        data_atualizacao
    FROM {{ ref('stg_clientes') }}
)

SELECT
   {{ dbt_utils.generate_surrogate_key(['cliente_id']) }} AS sk_cliente,
    cliente_id,
    nome_cliente,
    endereco,
    concessionaria_id,
    data_inclusao,
    data_atualizacao
FROM
clientes