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

FROM stocks_staging as stg
    INSERT INTO stocks PARTITION (symbol)
        SELECT stg.date_, stg.volume, stg.open, stg.close, stg.high, stg.low, stg.adjclose, stg.symbol
            WHERE SUBSTR(symbol, 0, 1) = "A"
        DISTRIBUTE BY symbol
    INSERT INTO stocks PARTITION (symbol)
        SELECT stg.date_, stg.volume, stg.open, stg.close, stg.high, stg.low, stg.adjclose, stg.symbol
            WHERE SUBSTR(symbol, 0, 1) = "B"
        DISTRIBUTE BY symbol
    INSERT INTO stocks PARTITION (symbol)
        SELECT stg.date_, stg.volume, stg.open, stg.close, stg.high, stg.low, stg.adjclose, stg.symbol
            WHERE SUBSTR(symbol, 0, 1) = "C"
        DISTRIBUTE BY symbol
    INSERT INTO stocks PARTITION (symbol)
        SELECT stg.date_, stg.volume, stg.open, stg.close, stg.high, stg.low, stg.adjclose, stg.symbol
            WHERE SUBSTR(symbol, 0, 1) = "D"
        DISTRIBUTE BY symbol
    INSERT INTO stocks PARTITION (symbol)
        SELECT stg.date_, stg.volume, stg.open, stg.close, stg.high, stg.low, stg.adjclose, stg.symbol
            WHERE SUBSTR(symbol, 0, 1) = "E"
        DISTRIBUTE BY symbol
    INSERT INTO stocks PARTITION (symbol)
        SELECT stg.date_, stg.volume, stg.open, stg.close, stg.high, stg.low, stg.adjclose, stg.symbol
            WHERE SUBSTR(symbol, 0, 1) = "F"
        DISTRIBUTE BY symbol
    INSERT INTO stocks PARTITION (symbol)
        SELECT stg.date_, stg.volume, stg.open, stg.close, stg.high, stg.low, stg.adjclose, stg.symbol
            WHERE SUBSTR(symbol, 0, 1) = "G"
        DISTRIBUTE BY symbol
     INSERT INTO stocks PARTITION (symbol)
        SELECT stg.date_, stg.volume, stg.open, stg.close, stg.high, stg.low, stg.adjclose, stg.symbol
            WHERE SUBSTR(symbol, 0, 1) = "H"
        DISTRIBUTE BY symbol
    INSERT INTO stocks PARTITION (symbol)
        SELECT stg.date_, stg.volume, stg.open, stg.close, stg.high, stg.low, stg.adjclose, stg.symbol
            WHERE SUBSTR(symbol, 0, 1) = "I"
        DISTRIBUTE BY symbol
    INSERT INTO stocks PARTITION (symbol)
        SELECT stg.date_, stg.volume, stg.open, stg.close, stg.high, stg.low, stg.adjclose, stg.symbol
            WHERE SUBSTR(symbol, 0, 1) = "J"
        DISTRIBUTE BY symbol
    INSERT INTO stocks PARTITION (symbol)
        SELECT stg.date_, stg.volume, stg.open, stg.close, stg.high, stg.low, stg.adjclose, stg.symbol
            WHERE SUBSTR(symbol, 0, 1) = "K"
        DISTRIBUTE BY symbol
    INSERT INTO stocks PARTITION (symbol)
        SELECT stg.date_, stg.volume, stg.open, stg.close, stg.high, stg.low, stg.adjclose, stg.symbol
            WHERE SUBSTR(symbol, 0, 1) = "L"
        DISTRIBUTE BY symbol
    INSERT INTO stocks PARTITION (symbol)
        SELECT stg.date_, stg.volume, stg.open, stg.close, stg.high, stg.low, stg.adjclose, stg.symbol
            WHERE SUBSTR(symbol, 0, 1) = "M"
        DISTRIBUTE BY symbol
    INSERT INTO stocks PARTITION (symbol)
        SELECT stg.date_, stg.volume, stg.open, stg.close, stg.high, stg.low, stg.adjclose, stg.symbol
            WHERE SUBSTR(symbol, 0, 1) = "N"
        DISTRIBUTE BY symbol
    INSERT INTO stocks PARTITION (symbol)
        SELECT stg.date_, stg.volume, stg.open, stg.close, stg.high, stg.low, stg.adjclose, stg.symbol
            WHERE SUBSTR(symbol, 0, 1) = "O"
        DISTRIBUTE BY symbol
    INSERT INTO stocks PARTITION (symbol)
        SELECT stg.date_, stg.volume, stg.open, stg.close, stg.high, stg.low, stg.adjclose, stg.symbol
            WHERE SUBSTR(symbol, 0, 1) = "P"
        DISTRIBUTE BY symbol
    INSERT INTO stocks PARTITION (symbol)
        SELECT stg.date_, stg.volume, stg.open, stg.close, stg.high, stg.low, stg.adjclose, stg.symbol
            WHERE SUBSTR(symbol, 0, 1) = "Q"
        DISTRIBUTE BY symbol
    INSERT INTO stocks PARTITION (symbol)
        SELECT stg.date_, stg.volume, stg.open, stg.close, stg.high, stg.low, stg.adjclose, stg.symbol
            WHERE SUBSTR(symbol, 0, 1) = "R"
        DISTRIBUTE BY symbol
    INSERT INTO stocks PARTITION (symbol)
        SELECT stg.date_, stg.volume, stg.open, stg.close, stg.high, stg.low, stg.adjclose, stg.symbol
            WHERE SUBSTR(symbol, 0, 1) = "S"
        DISTRIBUTE BY symbol
    INSERT INTO stocks PARTITION (symbol)
        SELECT stg.date_, stg.volume, stg.open, stg.close, stg.high, stg.low, stg.adjclose, stg.symbol
            WHERE SUBSTR(symbol, 0, 1) = "T"
        DISTRIBUTE BY symbol
    INSERT INTO stocks PARTITION (symbol)
        SELECT stg.date_, stg.volume, stg.open, stg.close, stg.high, stg.low, stg.adjclose, stg.symbol
            WHERE SUBSTR(symbol, 0, 1) = "U"
        DISTRIBUTE BY symbol
    INSERT INTO stocks PARTITION (symbol)
        SELECT stg.date_, stg.volume, stg.open, stg.close, stg.high, stg.low, stg.adjclose, stg.symbol
            WHERE SUBSTR(symbol, 0, 1) = "V"
        DISTRIBUTE BY symbol
    INSERT INTO stocks PARTITION (symbol)
        SELECT stg.date_, stg.volume, stg.open, stg.close, stg.high, stg.low, stg.adjclose, stg.symbol
            WHERE SUBSTR(symbol, 0, 1) = "W"
        DISTRIBUTE BY symbol
    INSERT INTO stocks PARTITION (symbol)
        SELECT stg.date_, stg.volume, stg.open, stg.close, stg.high, stg.low, stg.adjclose, stg.symbol
            WHERE SUBSTR(symbol, 0, 1) = "X"
        DISTRIBUTE BY symbol
    INSERT INTO stocks PARTITION (symbol)
        SELECT stg.date_, stg.volume, stg.open, stg.close, stg.high, stg.low, stg.adjclose, stg.symbol
            WHERE SUBSTR(symbol, 0, 1) = "Y"
        DISTRIBUTE BY symbol
    INSERT INTO stocks PARTITION (symbol)
        SELECT stg.date_, stg.volume, stg.open, stg.close, stg.high, stg.low, stg.adjclose, stg.symbol
            WHERE SUBSTR(symbol, 0, 1) = "Z"
        DISTRIBUTE BY symbol;

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
