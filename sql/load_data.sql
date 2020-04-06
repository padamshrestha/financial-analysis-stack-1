-- 0. Enable dynamic partition inserts and make optimisations

SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.optimize.sort.dynamic.partition=true;

-- 1. Drop tables if they already exist

DROP TABLE IF EXISTS stocks_staging;
DROP TABLE IF EXISTS stocks;
DROP TABLE IF EXISTS symbol_descriptions;

-- 2. Use staging tables to temporarily load the data

CREATE EXTERNAL TABLE stocks_staging (
  symbol VARCHAR(5),
  date_ DATE,
  volume INT,
  open FLOAT,
  close FLOAT,
  high FLOAT,
  low FLOAT,
  adjclose FLOAT
)
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    STORED AS TEXTFILE
    TBLPROPERTIES ("skip.header.line.count"="1");

LOAD DATA LOCAL INPATH '/data/stock_histories.csv'
    OVERWRITE INTO TABLE stocks_staging;

-- 3. Insert staged table into an optimised table

CREATE TABLE stocks (
   date_ DATE,
   volume INT,
   open FLOAT,
   close FLOAT,
   high FLOAT,
   low FLOAT,
   adjclose FLOAT
 )
     PARTITIONED BY (symbol VARCHAR(5))
     STORED AS ORC;

INSERT INTO TABLE stocks PARTITION (symbol) SELECT * FROM stocks_staging
    WHERE SUBSTR(symbol, 0, 1) = "A";
INSERT INTO TABLE stocks PARTITION (symbol) SELECT * FROM stocks_staging
    WHERE SUBSTR(symbol, 0, 1) = "B";
INSERT INTO TABLE stocks PARTITION (symbol) SELECT * FROM stocks_staging
    WHERE SUBSTR(symbol, 0, 1) = "C";
INSERT INTO TABLE stocks PARTITION (symbol) SELECT * FROM stocks_staging
    WHERE SUBSTR(symbol, 0, 1) = "D";
INSERT INTO TABLE stocks PARTITION (symbol) SELECT * FROM stocks_staging
    WHERE SUBSTR(symbol, 0, 1) = "E";
INSERT INTO TABLE stocks PARTITION (symbol) SELECT * FROM stocks_staging
    WHERE SUBSTR(symbol, 0, 1) = "F";
INSERT INTO TABLE stocks PARTITION (symbol) SELECT * FROM stocks_staging
    WHERE SUBSTR(symbol, 0, 1) = "G";
INSERT INTO TABLE stocks PARTITION (symbol) SELECT * FROM stocks_staging
    WHERE SUBSTR(symbol, 0, 1) = "H";
INSERT INTO TABLE stocks PARTITION (symbol) SELECT * FROM stocks_staging
    WHERE SUBSTR(symbol, 0, 1) = "I";
INSERT INTO TABLE stocks PARTITION (symbol) SELECT * FROM stocks_staging
    WHERE SUBSTR(symbol, 0, 1) = "J";
INSERT INTO TABLE stocks PARTITION (symbol) SELECT * FROM stocks_staging
    WHERE SUBSTR(symbol, 0, 1) = "K";
INSERT INTO TABLE stocks PARTITION (symbol) SELECT * FROM stocks_staging
    WHERE SUBSTR(symbol, 0, 1) = "L";
INSERT INTO TABLE stocks PARTITION (symbol) SELECT * FROM stocks_staging
    WHERE SUBSTR(symbol, 0, 1) = "M";
INSERT INTO TABLE stocks PARTITION (symbol) SELECT * FROM stocks_staging
    WHERE SUBSTR(symbol, 0, 1) = "N";
INSERT INTO TABLE stocks PARTITION (symbol) SELECT * FROM stocks_staging
    WHERE SUBSTR(symbol, 0, 1) = "O";
INSERT INTO TABLE stocks PARTITION (symbol) SELECT * FROM stocks_staging
    WHERE SUBSTR(symbol, 0, 1) = "P";
INSERT INTO TABLE stocks PARTITION (symbol) SELECT * FROM stocks_staging
    WHERE SUBSTR(symbol, 0, 1) = "Q";
INSERT INTO TABLE stocks PARTITION (symbol) SELECT * FROM stocks_staging
    WHERE SUBSTR(symbol, 0, 1) = "R";
INSERT INTO TABLE stocks PARTITION (symbol) SELECT * FROM stocks_staging
    WHERE SUBSTR(symbol, 0, 1) = "S";
INSERT INTO TABLE stocks PARTITION (symbol) SELECT * FROM stocks_staging
    WHERE SUBSTR(symbol, 0, 1) = "T";
INSERT INTO TABLE stocks PARTITION (symbol) SELECT * FROM stocks_staging
    WHERE SUBSTR(symbol, 0, 1) = "U";
INSERT INTO TABLE stocks PARTITION (symbol) SELECT * FROM stocks_staging
    WHERE SUBSTR(symbol, 0, 1) = "V";
INSERT INTO TABLE stocks PARTITION (symbol) SELECT * FROM stocks_staging
    WHERE SUBSTR(symbol, 0, 1) = "W";
INSERT INTO TABLE stocks PARTITION (symbol) SELECT * FROM stocks_staging
    WHERE SUBSTR(symbol, 0, 1) = "X";
INSERT INTO TABLE stocks PARTITION (symbol) SELECT * FROM stocks_staging
    WHERE SUBSTR(symbol, 0, 1) = "Y";
INSERT INTO TABLE stocks PARTITION (symbol) SELECT * FROM stocks_staging
    WHERE SUBSTR(symbol, 0, 1) = "Z";

-- 4. Delete now unnecessary staging table

DROP TABLE stocks_staging;

-- 5. Load in the symbol descriptions

CREATE TABLE IF NOT EXISTS symbol_descriptions (
  symbol VARCHAR(5),
  name STRING
)
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE
    TBLPROPERTIES ("skip.header.line.count"="1");

LOAD DATA LOCAL INPATH '/data/symbol_descriptions.txt'
    OVERWRITE INTO TABLE symbol_descriptions;
