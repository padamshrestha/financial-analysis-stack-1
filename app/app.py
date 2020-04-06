from os import getenv

from pyhive import hive

def get_name(conn, symbol):
    cursor = conn.cursor()
    cursor.execute('SELECT name FROM symbol_descriptions WHERE symbol="%s"' % symbol)
    name = cursor.fetchone()[0] # first index of a 1-element tuple

    return name

def main():
    symbol = getenv('SPARK_APPLICATION_ARGS')
    print("Querying symbol %s" % symbol)

    print("Connecting to Hive")
    conn = hive.Connection(host='hive-server')

    name = get_name(conn, symbol)
    print("Finding stock history of %s (%s)" % (name, symbol))

if __name__ == "__main__":
    print("Starting application")
    main()
