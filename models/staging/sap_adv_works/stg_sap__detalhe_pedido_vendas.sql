with
    detalhe_pe_vendas as(
        select
            cast(salesorderid as int) id_pedido_vendas
            --, salesorderdetailid
            , cast(productid as int) as id_produto
            --, castspecialofferid
            --, carriertrackingnumber
            , cast(orderqty as int) as qtd_pedido
            , cast(unitprice as numeric) as preco_unitario
            , cast(unitpricediscount as numeric) as preco_desconto_unitario
            --, cast(rowguid as string) as rowguid
            , date(modifieddate) as data_modificacao
        from {{ source('sap', 'salesorderdetail') }}
    )
select *
from detalhe_pe_vendas