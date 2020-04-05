from pyhive import hive

connection = hive.Connection(host='hive-server')

cursor = connection.cursor()
cursor.execute('SELECT * FROM stocks LIMIT 10')

print(cursor.fetchall())
