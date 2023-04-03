with
    pessoa as (
        select
        cast(businessentityid as int ) as id_comercial_pessoa
        --,cast(persontype as string) as tipo_pessoa
        --,cast(namestyle as string ) as estilo_nome
        --,cast(title as string) as titulo
        ,cast(firstname as string) as primeiro_nome
        ,cast(middlename as string) as nome_do_meio
        ,cast(lastname as string) as sobre_nome
        --,cast(suffix as string) as sufixo
        --,cast(emailpromotion as int) as email_promocao
        --,cast(additionalcontactinfo as string) as contato_adicional_info
        --,cast(demographics as string) as demografico
        --,cast(rowguid as string) as rowguid
        ,date(modifieddate) as data_modificacao
        from {{ source('sap', 'person') }}
    )

select *
from pessoa
