with
    clientes as(
        select
            cast(customerid as int) as id_cliente
            , cast(personid as int) as id_pessoa
            , cast(storeid as int) as loja_id
            , cast(territoryid as int) as id_territorio
            --, cast(rowguid as string) as rowguid
            , date(modifieddate) as data_modificacao
        from {{ source('sap', 'customer') }}
    )
select *
from clientes