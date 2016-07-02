import db
import mysql.connector
import draft

config = dict(user = 'root',
			  password = 'Password',
			  database = 'test',
			  host = '127.0.0.1',
			  port = '3306')

db._logger()
db.logger.info('start')


def insert(table,**kw):
	cls,v = zip(*kw.iteritems())
	print cls,v

insert('user',name = 'Test',email = 'test@example.com',password = 'password',image = 'about:blank')