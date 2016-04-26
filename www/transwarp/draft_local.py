import db
import mysql.connector

config = dict(user = 'root',
			  password = 'Password',
			  database = 'test',
			  host = '127.0.0.1',
			  port = '3306')

engine = mysql.connector.connect(**config)
cursor = engine.cursor()
cursor.execute('create TABLE USER (id INT PRIMARY KEY ,NAME text,email text,passwd text,last_modified real)')
cursor.close()
engine.close