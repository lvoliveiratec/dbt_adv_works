with
    cabeca_motivo_vendas as(
        select
            cast(salesorderid as int) as id_vendas
            , cast(salesreasonid as int) as id_motivo_vendas
            , date( modifieddate) as data_modificacao
        from {{ source('sap', 'salesorderheadersalesreason') }}
    )
select *
from cabeca_motivo_vendas