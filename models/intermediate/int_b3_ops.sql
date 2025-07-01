
{{config (
	materialized ='incremental'
) }}


WITH final as (
    SELECT *
    FROM 
    {{ ref('stg_b3_ops') }}
)

select
to_date(date_op, 'DD-MM-YYYY') as date_op,
id,
movement,
market,
to_date(
	case
		when expire_date = '-' then '31-12-2099'
		else expire_date
		end, 
	'DD-MM-YYYY') as date_expiration,
CASE 
	when brokerage_firm = 'XP INVESTIMENTOS CCTVM S/A'
	THEN 1
	WHEN brokerage_firm = 'C6 CORRETORA DE TITULOS E VALORES MOBILIARIOS LTDA'
	THEN 2
	END as brokerage_firm_id, -- might need to modifiy this part adding more ids (pkeys) for brokerage firms depending in your case; 
case
	when market = 'Mercado Fracionário'
	then SUBSTRING(asset, 1, length(asset)-1)
	else asset
	END	as asset,
quantity::numeric,
pu::numeric(6,2),
value::numeric(9,2)
from stg_b3_ops
{% if is_incremental() %}
WHERE to_date(date_op, 'DD-MM-YYYY') > (SELECT MAX(date_op) from {{ this }} )
{% endif %}


