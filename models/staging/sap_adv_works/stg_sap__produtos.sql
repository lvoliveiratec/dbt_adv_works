with
    produto as (
        select
        cast(productid  as int) as id_produto 
        ,cast(name as string) as nome_produto
        ,cast(productnumber as string) as numero_produto
        ,cast(makeflag as bool) as makeflag
        ,cast(finishedgoodsflag	 as bool) as finishedgoodsflag
        ,cast(color as string) as cor_produto
        ,cast(safetystocklevel as int) as nivel_estoque
        ,cast(reorderpoint as int) as ponto_reabastecimento
        ,cast(standardcost as numeric) as custo_padrao
        ,cast(listprice as numeric) as lista_precos
        ,cast(size as string) as tamanho 
        ,cast(sizeunitmeasurecode as string) as codigo_medida_tamanho
        ,cast(weightunitmeasurecode as string) as codigo_medida_peso
        ,cast(weight as numeric) as peso
        ,cast(daystomanufacture as int) as dias_fabricacao
        ,cast(productline as string) as linha_producao 
        ,cast(class as string) as classe 
        ,cast(style as string) as estilo
        ,cast(productsubcategoryid as int) as id_subcategoria
        ,cast(productmodelid as int) as id_modelo
        ,date(sellstartdate) as data_inicio_venda
        ,date(sellenddate) as data_final_venda
        ,cast(discontinueddate as int) as data_descontinuacao
        ,cast(rowguid as string) as rowguid
        ,date(modifieddate) as data_modificacao
        from {{ source('sap', 'product') }}
    )

select *
from produto
