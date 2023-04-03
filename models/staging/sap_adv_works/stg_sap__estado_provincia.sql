with
    estado as (
        select
        cast(stateprovinceid as int) as id_estado
        ,cast(stateprovincecode as string) as codego_estado
        ,cast(countryregioncode as string) as codego_regiao
        --,cast(isonlystateprovinceflag as bool) as flag_estado
        ,cast(name as string) as nome_estado
        ,cast(territoryid as int) as id_territorio
        --,cast(rowguid as string) as rowguid
        ,date(modifieddate) as data_modificacao
        from {{ source('sap', 'stateprovince') }}
    )

select *
from estado
