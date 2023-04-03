with
    cab_pedido_vendas as (
        select 
            cast(salesorderid as int) as id_pedido_vendas
            , cast(customerid as int) as id_cliente
            , cast(territoryid as int) as id_territorio
            , cast(shiptoaddressid as int) as id_end_envio
            , cast(creditcardid	as int) as id_cartao
            , date(orderdate) as data_pedido
            , date(duedate) as data_vencimento
            , date(shipdate) as data_envio
            , cast(status as int) as status
            , cast(subtotal	as numeric)	as sub_total
            , cast(totaldue as numeric) as total_devido
            , date(modifieddate) as data_modificacao
        from {{source('sap','salesorderheader')}}
    )
select *
from cab_pedido_vendas