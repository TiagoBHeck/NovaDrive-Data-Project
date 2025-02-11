{{ 
    config(
        schema='ANALYTICS',
        materialized='view',         
        tags=['vw_vendas', 'analytics']
        ) 
}}

WITH vendas AS(
    SELECT 
        venda_id,
        sk_veiculo,
        sk_concessionaria,
        sk_vendedor,
        sk_cliente,
        valor_venda,
        data_venda,
        data_inclusao,
        data_atualizacao
    FROM {{ ref('fct_vendas') }}
),

veiculos AS(
    SELECT
        sk_veiculo,   
        nome_veiculo,
        tipo,
        valor_sugerido
    FROM {{ ref('dim_veiculos') }}
),

concessionarias AS(
    SELECT
        sk_concessionaria,   
        nome_concessionaria,
        cidade_id
    FROM {{ ref('dim_concessionarias') }}
),

vendedores AS(
    SELECT
        sk_vendedor,   
        nome_vendedor   
    FROM {{ ref('dim_vendedores') }}
),

clientes AS(
    SELECT
        sk_cliente,    
        nome_cliente,
        endereco,
    FROM {{ ref('dim_clientes') }}
)

SELECT
    VE.venda_id,
    VE.valor_venda,
    CAST(TO_VARCHAR(CAST(VE.data_venda AS DATE), 'YYYYMMDD') AS INTEGER) AS data_venda_key,
    VE.data_venda,
    VE.data_inclusao,
    VE.data_atualizacao,
    VC.nome_veiculo,
    VC.tipo,
    VC.valor_sugerido,
    CO.nome_concessionaria,
    VD.nome_vendedor,
    CE.nome_cliente,
    CE.endereco
FROM
vendas VE
left join veiculos VC on (VE.sk_veiculo = VC.sk_veiculo)
left join concessionarias CO on (VE.sk_concessionaria = CO.sk_concessionaria)
left join vendedores VD on (VE.sk_vendedor = VD.sk_vendedor)
left join clientes CE on (VE.sk_cliente = CE.sk_cliente)