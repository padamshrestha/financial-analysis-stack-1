from os import getenv

from pyhive import hive
from pyspark import SparkContext
from pyspark.sql import SparkSession

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

if __name__ == "__main__":
    print("Starting application")

    print("Connecting to Hive")
    connection = hive.Connection(host='hive-server')

    print("Connecting to Spark")
    sc = SparkContext.getOrCreate()
    spark = SparkSession(sc)

    main()
