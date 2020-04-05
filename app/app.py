from pyhive import hive
from os import getenv

def main():
    print("Connecting to Hive")
    connection = hive.Connection(host='hive-server')

    cursor = connection.cursor()

    symbol = getenv('SPARK_APPLICATION_ARGS')
    cursor.execute('SELECT name FROM symbol_descriptions WHERE symbol="%s"' % symbol)
    name = cursor.fetchall()[0]
    print("Finding history of stock %s (%s)" % (symbol, name))

if __name__ == "__main__":
    print("Starting application")
    main()
