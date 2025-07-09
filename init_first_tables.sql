CREATE TABLE IF NOT exists 
wallet (
    "date" date,
    wallet_id int,
    brokerage_firm_id int,
    asset varchar(64),
    quantity numeric(12,8),
    pu numeric(12,8),
    value numeric(15,8)
);

----
CREATE TABLE cashflow (
    id SERIAL primary key,
    wallet_id int,
    account int, 
    vd int, --verification digit;
    bank_id int,
    agency int,
    date DATE,
    value numeric(15,2),
    origem_id int
);


create table b3_raw_ops (
id serial primary key,
"Data do Negócio" varchar(32), 
"Tipo de Movimentação" varchar(32), 
"Mercado" varchar(32), 
"Prazo/Vencimento" varchar(32), 
"Instituição" varchar(64), 
"Código de Negociação" varchar(32), 
"Quantidade" int, 
"Preço" numeric,
"Valor" numeric, 
wallet_id int
);



------
CREATE TABLE if not exists brokerage_firm (
    id int PRIMARY KEY,
    name varchar(64)
);


INSERT INTO 
    brokerage_firm 
VALUES
    (1, 'XP INVESTIMENTOS CCTVM S/A'),
    (2, 'C6 CORRETORA DE TITULOS E VALORES MOBILIARIOS LTDA');

