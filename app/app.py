from os import getenv
from datetime import date
import re

from pyhive import hive
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import udf
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression

@udf(IntegerType())
def date_string_to_ordinal(date_str):
    date_ = date.fromisoformat(date_str)
    ordinal = date_.toordinal()
    return ordinal

def make_lr_model(symbol):
    cursor = connection.cursor()
    cursor.execute('SELECT date_, adjclose FROM stocks WHERE symbol="%s" LIMIT 20' % symbol)
    stock_history = cursor.fetchall()

    initial_schema = StructType([
        StructField('date', StringType()),
        StructField('adjclose', FloatType())
    ])
    df = spark.createDataFrame(stock_history, schema=initial_schema)

    ordinal_dates = date_string_to_ordinal(df.date)
    df = df.withColumn('date', ordinal_dates)

    va = VectorAssembler(inputCols=['adjclose'], outputCol='features')
    df = va.transform(df)

    lr = LinearRegression(labelCol='date')
    lr_model = lr.fit(df)

    return lr_model

def get_name(symbol):
    cursor = connection.cursor()
    cursor.execute('SELECT name FROM symbol_descriptions WHERE symbol="%s"' % symbol)
    name = cursor.fetchone()[0] # first index of a 1-element tuple

    return name

def main():
    symbol = getenv('SPARK_APPLICATION_ARGS')
    print("Querying symbol %s" % symbol)

    name = get_name(symbol)

    print("Finding stock history of %s (%s)" % (name, symbol))

    model = make_lr_model(symbol)

if __name__ == "__main__":
    print("Starting application")

    print("Connecting to Hive")
    connection = hive.Connection(host='hive-server')

    print("Connecting to Spark")
    sc = SparkContext.getOrCreate()
    spark = SparkSession(sc)

    main()
