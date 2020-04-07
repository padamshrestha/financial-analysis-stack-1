from argparse import ArgumentParser
from os import getenv
from datetime import date
import re
import pickle

import redis
from pyhive import hive
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import udf
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression

# Assign these global vars at runtime
r = None
connection = None
sc = None
spark = None


def get_name(symbol):
    if not r.exists('symbol-' + symbol):
        cursor = connection.cursor()
        cursor.execute(
            'SELECT name FROM symbol_descriptions WHERE symbol="%s"' % symbol)
        name = cursor.fetchone()[0]  # first index of a 1-element tuple
        r.set('symbol-' + symbol, name)
        return name

    name = r.get('symbol-' + symbol)
    return name


@udf(IntegerType())
def date_string_to_ordinal(date_str):
    date_ = date.fromisoformat(date_str)
    ordinal = date_.toordinal()

    return ordinal


def make_lr_model(symbol):
    cursor = connection.cursor()
    cursor.execute(
        'SELECT date_, close FROM stocks WHERE symbol="%s" LIMIT 20' % symbol)
    stock_history = cursor.fetchall()

    initial_schema = StructType([
        StructField('date', StringType()),
        StructField('close', FloatType())
    ])
    df = spark.createDataFrame(stock_history, schema=initial_schema)

    ordinal_dates = date_string_to_ordinal(df.date)
    df = df.withColumn('date', ordinal_dates)

    va = VectorAssembler(inputCols=['close'], outputCol='features')
    df = va.transform(df)

    lr = LinearRegression(labelCol='date')
    lr_model = lr.fit(df)

    return lr_model

def predict_close(lr_model, date_ordinal):
    ordinal_schema = StructType([StructField('date', IntegerType())])
    df = spark.createDataFrame([date_ordinal], schema=ordinal_schema)


def main():
    parser = ArgumentParser()
    parser.add_argument('symbol',
                        help="Stock listing to model for")
    parser.add_argument('date',
                        type=date.fromisoformat,
                        help="Predict stock's price at <yyyy-mm-dd>")
    args_env = getenv('SPARK_APPLICATION_ARGS').split(sep=' ')
    args = parser.parse_args(args_env)

    global r, connection, sc, spark
    print("Connecting to Redis")
    r = redis.Redis(host='redis', db=0)
    print("Connecting to Hive")
    connection = hive.Connection(host='hive-server')
    print("Connecting to Spark")
    sc = SparkContext.getOrCreate()
    spark = SparkSession(sc)

    name = get_name(args.symbol)
    print("Finding stock history of %s (%s)" % (name, args.symbol))

    lr_model = make_lr_model(args.symbol)
    lr_summary = lr_model.summary
    print("Successfully built linear regression model:\n",
          "\t" + "Coefficient: %f" % lr_model.coefficients[0] + "\n",
          "\t" + "Intercept:   %f" % lr_model.intercept + "\n",
          "\t" + "RMSE:        %f" % lr_summary.rootMeanSquaredError + "\n",
          "\t" + "r2:          %f" % lr_summary.r2)

    close = predict_close(lr_model, args.date.toordinal())
    print("Estimated price of %s at %s: $%s"
          % (symbol, date.isoformat(), '$'+format(close, ',.2f')))


if __name__ == "__main__":
    print("Starting application")

    main()
