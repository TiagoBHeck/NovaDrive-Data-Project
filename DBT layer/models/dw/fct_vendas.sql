{{ 
    config(
        schema='DW',
        materialized='incremental', 
        unique_key='venda_id',
        tags=['fct_vendas', 'dw']
        ) 
}}

WITH vendas AS (
    SELECT
        v.id_vendas AS venda_id,
        vei.sk_veiculo,
        con.sk_concessionaria,
        ven.sk_vendedor,
        cli.sk_cliente,
        v.valor_venda, 
        v.data_venda,
        v.data_inclusao,
        v.data_atualizacao
    FROM {{ ref('stg_vendas') }} v
    JOIN {{ ref('dim_veiculos') }} vei ON v.id_veiculos = vei.veiculo_id
    JOIN {{ ref('dim_concessionarias') }} con ON v.id_concessionarias = con.concessionaria_id
    JOIN {{ ref('dim_vendedores') }} ven ON v.id_vendedores = ven.vendedor_id
    JOIN {{ ref('dim_clientes') }} cli ON v.id_clientes = cli.cliente_id
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['venda_id']) }} AS sk_venda,
    venda_id,
    sk_veiculo,
    sk_concessionaria,
    sk_vendedor,
    sk_cliente,
    valor_venda,
    data_venda,
    data_inclusao,
    data_atualizacao
FROM vendas
{% if is_incremental() %}
    WHERE venda_id > (SELECT MAX(venda_id) FROM {{ this }})
{% endif %}