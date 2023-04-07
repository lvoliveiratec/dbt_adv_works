with
    enderecos as (
        select
            id_endereco
            , cidade
            , id_estado
        from {{ ref('stg_sap__enderecos') }}
    )
    , estado_provincia as (
        select
            id_estado
            , codego_estado
            , nome_estado
            , codego_regiao
        from {{ ref('stg_sap__estado_provincia') }}
    )
    , regiao as (
        select
            codego_cidade
            ,nome_pais
        from {{ ref('stg_sap__regiao') }}
    )
    , joined as (
        select
            enderecos.id_endereco
            , enderecos.id_estado
            , enderecos.cidade
            , estado_provincia.codego_estado
            , estado_provincia.nome_estado
            , estado_provincia.codego_regiao
            , regiao.nome_pais
        from enderecos
        left join estado_provincia on enderecos.id_estado = estado_provincia.id_estado
        left join regiao on estado_provincia.codego_regiao = regiao.codego_cidade
    )
    , endereco_sk as (
        select
            {{dbt_utils.surrogate_key(['id_endereco']) }} as endereco_sk
            , *
        from joined
    )
select *
from endereco_sk