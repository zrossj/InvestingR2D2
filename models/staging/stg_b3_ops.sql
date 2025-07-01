
with final as (
    SELECT * FROM {{ source('raw_data_table', 'b3_raw_ops') }}
)

SELECT 
"Data do Negócio" as date_op,
"id",
"Tipo de Movimentação" as movement,
"Mercado" as market,
"Prazo/Vencimento" as expire_date,
"Instituição" as brokerage_firm,
"Código de Negociação" as asset,
"Quantidade" as quantity, 
"Preço" as pu,
"Valor" as value 
FROM final
