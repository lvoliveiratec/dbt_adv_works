with 
    clientes as (
        select 
            id_cliente
            , id_pessoa
            , loja_id
            , id_territorio
        from {{ ref('stg_sap__clientes') }}
    )
    , pessoas as (
        select 
            id_comercial_pessoa
            , concat(primeiro_nome, " ", sobre_nome) as nome_pessoa
        from {{ ref('stg_sap__pessoa') }}
    )
    , lojas as (
        select
            id_loja
            , nome_loja
        from {{ ref('stg_sap__lojas') }}
    )
    , joined as (
      select 
          clientes.id_cliente
          , case
              when clientes.loja_id is not null then lojas.nome_loja
              else pessoas.nome_pessoa
          end as nome_clientes
          , clientes.id_territorio
      from clientes
      left join pessoas on clientes.id_pessoa = pessoas.id_comercial_pessoa
      left join lojas on clientes.loja_id = lojas.id_loja
    )
    , clientes_sk as (
        select
            {{ dbt_utils.surrogate_key(['id_cliente']) }} as clientes_sk
            , id_cliente
            , nome_clientes
            , id_territorio
        from joined
    )
select *
from clientes_sk