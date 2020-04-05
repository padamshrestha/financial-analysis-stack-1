from pyhive import hive
from os import getenv

def main():
    print("Connecting to Hive")
    connection = hive.Connection(host='hive-server')

    cursor = connection.cursor()

    symbol = getenv('SPARK_APPLICATION_ARGS')
    cursor.execute('SELECT name FROM symbol_descriptions WHERE symbol="%s"' % symbol)
    name = cursor.fetchall()[0][0] # Get name from a list of 1-element tuples
    print("Finding stock history of %s (%s)" % (name, symbol))

if __name__ == "__main__":
    print("Starting application")
    main()
