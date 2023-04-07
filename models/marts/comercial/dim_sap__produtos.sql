with
    produto as (
        select
            {{ dbt_utils.surrogate_key(['id_produto']) }} as sk_produto
            , id_produto
            , nome_produto
            , numero_produto
        from {{ ref('stg_sap__produtos') }}
    )
select *
from produto