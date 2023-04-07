with
    dim_endereco as (
        select
            endereco_sk
            , id_endereco
            , id_estado
            ,cidade
            ,nome_estado
            ,nome_pais
        from {{ ref('dim_sap__enderecos') }}
    )
    , dim_cartoes as (
        select
            sk_cartao_credito
            , id_cartao
            ,tipo_cartao
        from {{ ref('dim_sap__cartoes') }}
    )
    , dim_clientes as (
        select
            clientes_sk
            ,nome_clientes
            , id_cliente
        from {{ ref('dim_sap__clientes') }}
    )
    , dim_produtos as (
        select
            sk_produto
            , id_produto
            ,nome_produto
        from {{ ref('dim_sap__produtos') }}
    )
    , dim_motivo_vendas as (
        select
            sk_motivo_vendas
            , id_vendas
            ,nome as motivo_venda
        from {{ ref('dim_sap__motivo_vendas') }}
    )
    , detalhe_pedido_vendas as (
        select
            id_pedido_vendas
            , id_produto
            , qtd_pedido
            , preco_unitario
            , preco_desconto_unitario
        from {{ ref('stg_sap__detalhe_pedido_vendas') }}
    )
    , cab_pedido_vendas as (
        select
            id_pedido_vendas
            , id_cliente
            , id_territorio
            , id_end_envio
            , id_cartao
            , data_pedido
            , data_vencimento
            , data_envio
            , status
            , sub_total
            , total_devido
        from {{ ref('stg_sap__cab_pedido_vendas') }}            
    )
    , joined as (
        select
            detalhe_pedido_vendas.id_pedido_vendas
            , cab_pedido_vendas.id_cliente
            , cab_pedido_vendas.id_territorio
            ,cab_pedido_vendas.id_end_envio
            , cab_pedido_vendas.id_cartao
            , cab_pedido_vendas.data_pedido
            , cab_pedido_vendas.data_vencimento -- Criar dim_dates
            , cab_pedido_vendas.data_envio
            , cab_pedido_vendas.status
            , detalhe_pedido_vendas.id_produto
            , detalhe_pedido_vendas.qtd_pedido
            , detalhe_pedido_vendas.preco_unitario
            , detalhe_pedido_vendas.preco_desconto_unitario
            , cab_pedido_vendas.sub_total
            , cab_pedido_vendas.total_devido
            ,dim_motivo_vendas.motivo_venda
            ,dim_motivo_vendas.sk_motivo_vendas
        from detalhe_pedido_vendas
        left join cab_pedido_vendas on detalhe_pedido_vendas.id_pedido_vendas = cab_pedido_vendas.id_pedido_vendas
        left join dim_motivo_vendas on cab_pedido_vendas.id_pedido_vendas = dim_motivo_vendas.id_vendas
    )
    , gera_sk as ( 
        select
            {{ dbt_utils.surrogate_key(['id_pedido_vendas']) }} as sk_detalhe_pedido_vendas
            , joined.id_pedido_vendas
            , dim_clientes.clientes_sk
            ,dim_clientes.nome_clientes
            , dim_endereco.endereco_sk
            ,dim_endereco.cidade
            ,dim_endereco.nome_estado
            ,dim_endereco.nome_pais
            , dim_cartoes.sk_cartao_credito
            ,dim_cartoes.tipo_cartao
            , dim_produtos.sk_produto
            , dim_produtos.nome_produto
            , joined.data_pedido
            , joined.data_vencimento
            , joined.data_envio
            , joined.status
            , joined.qtd_pedido
            , joined.preco_unitario
            , joined.preco_desconto_unitario
            , joined.sub_total
            , joined.total_devido
            ,joined.motivo_venda
            ,joined.sk_motivo_vendas
        from joined
        left join dim_endereco on joined.id_end_envio = dim_endereco.id_endereco
        left join dim_cartoes on joined.id_cartao = dim_cartoes.id_cartao
        left join dim_clientes on joined.id_cliente = dim_clientes.id_cliente
        left join dim_produtos on joined.id_produto = dim_produtos.id_produto
        
    )
select * 
from gera_sk
