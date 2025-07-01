CREATE TABLE IF NOT exists 
wallet (
    "date" date,
    id int,
    brokerage_firm_id int,
    asset varchar(64),
    quantity numeric(12,8),
    pu numeric(12,8),
    value numeric(15,8)
);



CREATE TABLE if not exists brokerage_firm (
    id int PRIMARY KEY,
    name varchar(64)
);


INSERT INTO 
    brokerage_firm 
VALUES
    (1, 'XP INVESTIMENTOS CCTVM S/A'),
    (2, 'C6 CORRETORA DE TITULOS E VALORES MOBILIARIOS LTDA');


CREATE TABLE cashflow (
    id SERIAL int primary key,
    wallet_id int,
    account int, 
    vd int, --verification digit;
    bank_id int,
    agency int,
    date DATE,
    value numeric(15,2)
);