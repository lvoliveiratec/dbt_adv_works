with
    regiao as (
        select
            cast(countryregioncode as string) as codego_cidade	
            , cast(name as string) as nome_pais			
            , date(modifieddate) data_modificacao
        from {{ source('sap', 'countryregion') }}
    )

select *
from regiao
