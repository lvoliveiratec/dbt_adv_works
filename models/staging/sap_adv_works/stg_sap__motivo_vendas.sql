with
    motivo_vendas as(
        select
            cast(salesreasonid as int) as id_motivo_vendas
            , cast(name as string) as nome
            , cast(reasontype as  string) as tipo_motivo
            , date(modifieddate) as data_modificacao
        from {{ source('sap', 'salesreason') }}
    )
select *
from motivo_vendas