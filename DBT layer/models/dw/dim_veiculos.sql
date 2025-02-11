{{ 
    config(
        schema='DW',
        materialized='table',
        unique_key='sk_veiculo',
        tags=['dim_veiculos', 'dw']
    )
}}

WITH veiculos AS (
    SELECT
        id_veiculos AS veiculo_id,
        nome AS nome_veiculo,
        tipo,
        valor AS valor_sugerido,
        data_atualizacao,
        data_inclusao
    FROM {{ ref('stg_veiculos') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['veiculo_id']) }} AS sk_veiculo,
    veiculo_id,
    nome_veiculo,
    tipo,
    valor_sugerido,
    data_atualizacao,
    data_inclusao
FROM veiculos