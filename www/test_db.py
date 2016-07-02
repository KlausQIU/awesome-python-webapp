# !/usr/env/bin python
# -*- coding:utf-8 -*-

from models import User, Blog, Comment

from transwarp import db

db._logger()
db.create_engine(user='root', password='Password', database='awesome')
u1 = User.find_first('where email=?', 'test@example.com3')
print 'find user\'s name:', u1.name

u2 = User.find_first('where email=?', 'test@example.com2')
print 'find user:', u2





