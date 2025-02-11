{{ 
    config(
        schema='STAGING',
        materialized='table',
        tags=['stg_vendas', 'staging']
    ) 
}}

WITH source AS (
    SELECT
        id_vendas,
        id_veiculos,
        id_concessionarias,
        id_vendedores,
        id_clientes,
        valor_pago::DECIMAL(10,2) AS valor_venda, 
        data_venda,
        data_inclusao,
        COALESCE(data_atualizacao, data_venda) AS data_atualizacao
    FROM {{ source('sources', 'VENDAS') }}
)

SELECT
    id_vendas,
    id_veiculos,
    id_concessionarias,
    id_vendedores,
    id_clientes,
    valor_venda,
    data_venda,
    data_inclusao,
    data_atualizacao
FROM source