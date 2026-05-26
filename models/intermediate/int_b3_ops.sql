
{{config (
	materialized ='table'
) }}


WITH final as (
    SELECT *
    FROM 
    {{ ref('stg_b3_ops') }}
)

select
id,
to_date(date, 'DD-MM-YYYY') as date,
wallet_id,
movement,
CASE
    WHEN 
        event_type = 'Juros Sobre Capital Próprio' 
    THEN
        'JCP'
    ELSE 
        event_type
END as event_type,
CASE 
	when brokerage_firm = 'XP INVESTIMENTOS CCTVM S/A'
	THEN 1
    WHEN brokerage_firm = 'XP INVESTIMENTOS CORRETORA DE CAMBIO TITULOS E VALORES MOBILIARIOS S/A'
    THEN 2
	WHEN brokerage_firm = 'C6 CORRETORA DE TITULOS E VALORES MOBILIARIOS LTDA'
	THEN 3
	WHEN brokerage_firm = 'BANCO C6 S.A.'
	THEN 4    
    WHEN brokerage_firm = 'SANTANDER CCVM S/A'
    THEN 5  
    WHEN brokerage_firm = 'ITAU CV S/A'
    THEN 6
    WHEN brokerage_firm = 'CLEAR CORRETORA - GRUPO XP'
    THEN 2 -- as clear moved to be called xp invest corretora de c....
    WHEN brokerage_firm = 'NU INVEST CORRETORA DE VALORES S.A.'
    THEN 8
    WHEN brokerage_firm = 'BANCO BTG PACTUAL S/A'
    THEN 9
END as brokerage_firm_id, -- might need to be modify this part adding more ids (pkeys) for brokerage firms depending in your case; 
CASE
    WHEN substring(asset, 1,5) = 'Opção' 
        THEN SPLIT_PART(asset, ' - ', 2)
    WHEN substring(asset, 1,7) = 'Tesouro'
        THEN asset
    ELSE 
        SPLIT_PART(asset, ' - ', 1)
END as asset, -- mercado normal, ticker na p1 do split. ex: "PINE4 - BANCO PINE"
quantity::numeric,
CASE
    WHEN pu = '-'
    THEN '0'
    ELSE pu
END::numeric as pu,
CASE
    WHEN total_amount = '-'
    THEN '0'
    ELSE total_amount
END::numeric as total_amount
from stg_b3_ops
{% if is_incremental() %}
WHERE to_date(date_op, 'DD-MM-YYYY') > (SELECT MAX(date_op) from {{ this }} )
{% endif %}


