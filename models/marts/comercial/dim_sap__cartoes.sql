with
    cartoes as (
        select
            id_cartao
            , tipo_cartao
            , data_modificacao
        from {{ ref('stg_sap__cartoes') }}
    )
    , retira_duplicados as (
        select
            row_number() over(
                partition by
                    id_cartao
                    , tipo_cartao
                order by data_modificacao desc
            ) as cartao_credito_rn
            , id_cartao
            , tipo_cartao
        from cartoes
        qualify cartao_credito_rn = 1
    )
    , tranformacao as (
        select
            {{ dbt_utils.surrogate_key(['id_cartao', 'tipo_cartao']) }} as sk_cartao_credito
            , id_cartao
            , tipo_cartao
        from retira_duplicados
    )
select *
from tranformacao
