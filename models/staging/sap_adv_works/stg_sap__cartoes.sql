
with
    cartao as (
        select
        cast(creditcardid as int) as id_cartao
        ,cast(cardtype as string) as tipo_cartao
        ,cast(cardnumber as int) as numero_cartao
        ,cast(expmonth as int) as expmonth
        ,cast(expyear as int) as expyear
        ,date(modifieddate) as data_modificacao
        from {{ source('sap', 'creditcard') }}
    )

select *
from cartao
	