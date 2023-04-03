with
    lojas as(
        select
            cast(businessentityid as int) as id_loja
            , cast(name as string) as nome_loja
            , cast(salespersonid as int) as id_vendedor
        from {{ source('sap', 'store') }}
    )
select *
from lojas