DROP TABLE IF exists wallet, cashflow, bronze_b3_ops, brokerage_firm;

CREATE TABLE wallet (
    "date" date,
    wallet_id int,
    brokerage_firm_id int,
    asset varchar(64),
    quantity numeric(13,8),
    pu numeric(7,2),
    total_amount numeric(7,2)
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
    total_amount numeric(7,2),
    origem_id int
);


create table bronze_b3_ops (
id serial primary key,
wallet_id int,
"Entrada/Saída" varchar(16), 
"Data" varchar(16), 
"Movimentação" varchar(128), 
"Produto" varchar(128), 
"Instituição" varchar(128),  
"Quantidade" varchar(128),
"Preço unitário" varchar(128), 
"Valor da Operação" varchar(128)
);



------
CREATE TABLE brokerage_firm (
    id int PRIMARY KEY,
    name varchar(128)
);


INSERT INTO 
    brokerage_firm 
VALUES
	(1, 'XP INVESTIMENTOS CCTVM S/A'),
	(2, 'XP INVESTIMENTOS CORRETORA DE CAMBIO TITULOS E VALORES MOBILIARIOS S/A'),
    (3, 'C6 CORRETORA DE TITULOS E VALORES MOBILIARIOS LTDA'),
	(4, 'BANCO C6 S.A.'),
	(5, 'SANTANDER CCVM S/A'),
    (6, 'ITAU CV S/A'),
    (7, 'CLEAR CORRETORA - GRUPO XP'),
    (8, 'NU INVEST CORRETORA DE VALORES S.A.'),
    (9, 'BANCO BTG PACTUAL S/A');
    