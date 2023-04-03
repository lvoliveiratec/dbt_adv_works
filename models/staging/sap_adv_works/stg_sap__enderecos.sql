with
    enderecos as (
        select
            cast(addressid as int) as id_endereco
            , cast(stateprovinceid as int) as id_estado
            --, cast(rowguid as string) as rowguid
            , cast(postalcode as string) as cep
            , cast(addressline1 as string) as endereco1
            , cast(addressline2 as string) as endereco2
            , cast(city as string) as cidade
            --, cast(spatiallocation as string) as localizacao
            , date(modifieddate) as data_modificacao

        from {{ source('sap', 'address') }}
    )

select *
from enderecos
