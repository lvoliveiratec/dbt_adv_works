with
    cabeca_motivo_vendas as (
        select
            id_vendas
            , id_motivo_vendas
        from {{ ref('stg_sap__cabeca_motivo_vendas') }}
    )
    , motivo_vendas as (
        select
            id_motivo_vendas
            , nome
            , tipo_motivo
        from {{ ref('stg_sap__motivo_vendas') }}
    )
    , joined as (
        select
            {{ dbt_utils.surrogate_key(['id_vendas']) }} as sk_motivo_vendas
            , id_vendas
            , nome
            , tipo_motivo
        from cabeca_motivo_vendas
        left join motivo_vendas on cabeca_motivo_vendas.id_motivo_vendas = motivo_vendas.id_motivo_vendas
    )
select *
from joined