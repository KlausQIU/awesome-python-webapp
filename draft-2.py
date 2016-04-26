# !/usr/env/bin python
# -*- coding:utf-8 -*-
import mysql.connector
import

params = dict(user = 'root',password = 'Password',database = 'test',use_unicode = True)
conn = mysql.connector.connect(**params)
cursor = conn.cursor()
cursor.execute('drop table if exists book ')
cursor.execute('drop table if exists USER ')
cursor.execute('create table USER (id int PRIMARY key,name text,email text,passwd text,last_modified REAL )')