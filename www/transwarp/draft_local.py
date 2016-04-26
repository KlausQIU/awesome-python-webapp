import db
import mysql.connector
import draft

config = dict(user = 'root',
			  password = 'Password',
			  database = 'test',
			  host = '127.0.0.1',
			  port = '3306')

db._logger()
db.create_engine(**config)
user = db.select('select * from USER ',True)
print user
