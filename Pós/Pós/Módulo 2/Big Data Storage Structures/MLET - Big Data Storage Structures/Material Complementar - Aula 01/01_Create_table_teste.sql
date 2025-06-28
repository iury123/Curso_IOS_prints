/*=== [ Demo 01 ] =============================================================*/

CREATE TABLE users (
  userid INTEGER NOT NULL,
  username VARCHAR(30),
  firstname VARCHAR(30),
  region VARCHAR(30),
  age INTEGER
);

select count(1) as qd from users;

COPY users FROM 's3://bucket-fiap-redshift-2024/AmostraDados_s3.csv' 
CREDENTIALS 'aws_iam_role=arn:aws:iam::275348310954:role/Redshift-Role'
DELIMITER ',';

select * from pg_catalog.stl_load_errors;

COPY users FROM 's3://bucket-fiap-redshift-2024/AmostraDados_s3.csv' 
CREDENTIALS 'aws_iam_role=arn:aws:iam::275348310954:role/Redshift-Role'
DELIMITER ';';

/*=== [ Carrega dados de pedidos ] =============================================================*/

CREATE TABLE itens_pedidos (
    id_nf INTEGER,
    produto_id INTEGER,
    pedido_id INTEGER,
    quantidade INTEGER,
    valor_unitario NUMERIC(10,2),
    valor_total NUMERIC(10,2),
    Estado VARCHAR(5),
    frete NUMERIC(10,2)
);

COPY itens_pedidos FROM 's3://bucket-fiap-redshift-2024/itens_pedidos.csv' 
CREDENTIALS 'aws_iam_role=arn:aws:iam::275348310954:role/Redshift-Role'
DELIMITER ','
IGNOREHEADER 1;

select * from itens_pedidos;

CREATE TABLE pedidos (
    pedido_id INTEGER,
    produto_id INTEGER,
    vendedor_id INTEGER,
    data_compra DATE,
    total NUMERIC(12,2)
);

COPY pedidos FROM 's3://bucket-fiap-redshift-2024/pedidos.csv' 
CREDENTIALS 'aws_iam_role=arn:aws:iam::275348310954:role/Redshift-Role'
DELIMITER ','
IGNOREHEADER 1;

select * from pedidos;

CREATE TABLE produtos (
    produto_id INTEGER,
    produto VARCHAR(255),
    preco INTEGER,
    marca VARCHAR(255),
    sku VARCHAR(50),
    Condicao VARCHAR(50)
);

COPY produtos FROM 's3://bucket-fiap-redshift-2024/produtos.csv' 
CREDENTIALS 'aws_iam_role=arn:aws:iam::275348310954:role/Redshift-Role'
DELIMITER ';'
IGNOREHEADER 1;

select * from produtos;

CREATE TABLE vendedores (
    vendedor_id INTEGER,
    nome_vendedor VARCHAR(255)
);

COPY vendedores FROM 's3://bucket-fiap-redshift-2024/vendedores.csv' 
CREDENTIALS 'aws_iam_role=arn:aws:iam::275348310954:role/Redshift-Role'
DELIMITER ','
IGNOREHEADER 1;

select * from vendedores;

/*=== [ Analisando os dados] =======================================================================================*/

--Quais são as condições e as quantidades de produtos ofertados em cada condição?
SELECT 
	p.Condicao, 
  AVG(p.preco) AS preco_medio
FROM produtos p
GROUP BY p.Condicao
ORDER BY AVG(p.preco) DESC;

--Quais são os produtos mais vendidos, em quantidade e em valor? 
SELECT 
	  p.produto, 
    SUM(ip.quantidade) AS quantidade_total, 
    SUM(ip.valor_total) AS valor_total
FROM itens_pedidos ip JOIN produtos p ON ip.produto_id = p.produto_id
GROUP BY p.produto
ORDER BY SUM(ip.valor_total) DESC;

--Nas vendas da nossa loja online existe alguma condição de produtos que mais se destaca?
SELECT 
    p.condicao,
    SUM(ip.quantidade) AS quantidade_total, 
    SUM(ip.valor_total) AS valor_total
FROM itens_pedidos ip JOIN produtos p ON ip.produto_id = p.produto_id
GROUP BY  p.condicao
ORDER BY SUM(ip.valor_total) DESC;

--Quem são os melhores vendedores?
SELECT 
	v.nome_vendedor, 
    SUM(p.total) AS valor_total_vendas
FROM pedidos p
JOIN vendedores v ON p.vendedor_id = v.vendedor_id
GROUP BY v.nome_vendedor
ORDER BY SUM(p.total) DESC

-- Qual é a o volume das vendas ao longo dos meses?
SELECT 
	EXTRACT(YEAR FROM data_compra) AS ano, 
    EXTRACT(MONTH FROM data_compra) AS mes, 
    SUM(total) AS valor_total_vendas
FROM pedidos
GROUP BY 
	EXTRACT(YEAR FROM data_compra),
  EXTRACT(MONTH FROM data_compra)
ORDER BY 
  ano desc, 
  mes desc;

-- Quais são os valores de vendas por Estado?
SELECT 
	ip.Estado, 
  SUM(ip.valor_total) AS valor_total_vendas
FROM itens_pedidos ip
GROUP BY ip.Estado
ORDER BY SUM(ip.valor_total) DESC;

-- Quais são as condições e as quantidades de produtos vendidos em cada condição?
SELECT 
	p.Condicao, 
    AVG(p.preco) AS preco_medio
FROM produtos p
GROUP BY p.Condicao
ORDER BY AVG(p.preco) DESC;

/*=== [ dados de voos americanos ] =========================================================================================*/

--Cria tabela flights
CREATE TABLE flights (
  year           smallint,
  month          smallint,
  day            smallint,
  carrier        varchar(80) DISTKEY,
  origin         char(3),
  dest           char(3),
  aircraft_code  char(3),
  miles          int,
  departures     int,
  minutes        int,
  seats          int,
  passengers     int,
  freight_pounds int
);

COPY flights
FROM 's3://us-west-2-aws-training/courses/spl-17/v4.2.15.prod-90ac2409/data/flights-usa'
IAM_ROLE 'arn:aws:iam::275348310954:role/Redshift-Role'
GZIP
DELIMITER ','
REMOVEQUOTES
REGION 'us-west-2';

--Exibe algumas linhas
SELECT * FROM flights ORDER BY random() LIMIT 10;

--Contagem: 96.825.753 registros
SELECT COUNT(*) FROM flights;

--Cria tabela aircraft
CREATE TABLE aircraft (
  aircraft_code CHAR(3) SORTKEY,
  aircraft      VARCHAR(100)
);

COPY aircraft
FROM 's3://us-west-2-aws-training/courses/spl-17/v4.2.15.prod-90ac2409/data/lookup_aircraft.csv'
IAM_ROLE 'arn:aws:iam::275348310954:role/Redshift-Role'
IGNOREHEADER 1
DELIMITER ','
REMOVEQUOTES
TRUNCATECOLUMNS
REGION 'us-west-2';

--Exibe algumas linhas
SELECT * FROM aircraft ORDER BY random() LIMIT 10;

--Contagem: 383 registros
SELECT COUNT(*) FROM aircraft;

--Quais são as aeronaves com mais voos?
SELECT
  aircraft,
  SUM(departures) AS trips
FROM flights
JOIN aircraft using (aircraft_code)
GROUP BY aircraft
ORDER BY trips DESC
LIMIT 10;

--Cria tabela airports
CREATE TABLE airports (
  airport_code CHAR(3) SORTKEY,
  airport      varchar(100)
);

COPY airports
FROM 's3://us-west-2-aws-training/courses/spl-17/v4.2.15.prod-90ac2409/data/lookup_airports.csv'
IAM_ROLE 'arn:aws:iam::275348310954:role/Redshift-Role'
IGNOREHEADER 1
DELIMITER ','
REMOVEQUOTES
TRUNCATECOLUMNS
REGION 'us-west-2';

--Exibe algumas linhas
SELECT * FROM airports ORDER BY random() LIMIT 10;

--Contagem: 6.265 registros
SELECT COUNT(*) FROM airports;


-- Cria view de voos apenas com destino a LAS VEGAS
CREATE VIEW vegas_flights AS
SELECT
  flights.*,
  airport
FROM flights
JOIN airports ON origin = airport_code
WHERE dest = 'LAS';

SELECT * FROM vegas_flights LIMIT 100

-- Quais são os voos mais populares com origem a las vegas?
SELECT
  airport,
  to_char(SUM(passengers), '999,999,999') as passengers
FROM vegas_flights
GROUP BY airport
ORDER BY SUM(passengers) desc
LIMIT 10;
