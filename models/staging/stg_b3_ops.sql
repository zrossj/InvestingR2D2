
with final as (
    SELECT * FROM {{ source('raw_data_table', 'bronze_b3_ops') }}
)

SELECT
id,
wallet_id,
"Entrada/Saída" as movement,
"Data" as date,  
"Movimentação" as event_type, 
"Produto" as asset, 
"Instituição" as brokerage_firm,  
"Quantidade" as quantity,
"Preço unitário" as pu,
"Valor da Operação" as total_amount
FROM final