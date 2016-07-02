# !/usr/bin/env python
# -*- coding:utf-8 -*-
import threading
import logging
import functools
import time
import uuid
import os



__author__ = 'KlausQiu'

#定义日志文件
logger = None

#定义一个通过key可以得到value的字典
class Dict(dict):
	def __init__(self,names=(),values=(),**kw):
		#注意使用super继承父类
		super(Dict,self).__init__(**kw)
		for k,v in zip(names,values):
			self[k] = v

#key判断
	def __getattr__(self, key):
		try:
			return self[key]
		except KeyError:
			raise AttributeError(r"'Dict' object has no attribute '%s'"%key)

#设置实例
	def __setattr__(self, key, value):
			self[key] = value

def next_id(t=None):
	#返回唯一ID值。
	if t is None:
		t = time.time()
	return '%015d%s' %(int(t * 1000),uuid.uuid4().hex)

#设置日志文件
def _logger():
	global logger
	logger = logging.getLogger('DBlog')
	fh = logging.FileHandler('./DB.log')
	format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
	fh.setFormatter(format)
	fh.setLevel(logging.DEBUG)
	logger.addHandler(fh)
	logger.info('[CREATE DB.log]')

#记录sql执行信息
def _profiling(start,sql=''):
	'''
	:param start:time.time()
	:param sql:sql command
	:return:record db log
	'''
	global logger
	t = time.time() - start
	if t > 0.1:
		logger.warning('[PROFILING] [DB]%s:%s'%(t,sql))
	else:
		logger.info('[PROFILING] [DB]%s:%s'%(t,sql))



class DBError(Exception):
	pass

class MultiColumnsError(DBError):
	pass

#惰性连接，自动连接与关闭
class _LasyConnection(object):
	def __init__(self):
		self.connection = None

#游标
	def cursor(self):
		global logger,engine
		if self.connection is None:
			conneciton = engine.connect()
			logger.info('[CONNECTION] [OPEN] connection <%s>...'% hex(id(conneciton)))
			self.connection = conneciton
		return conneciton.cursor()

#提交
	def commit(self):
		return self.connection.commit()

#回滚
	def rollback(self):
		return self.connection.rollback()

#关闭连接
	def cleanup(self):
		global logger
		if self.connection:
			connection = self.connection
			self.connection = None
			logger.info('[CONNECTION] [CLOSE] connection <%s>..' % hex(id(connection)))
			connection.close()

#数据库连接
class _DbCtx(threading.local):
	def __init__(self):
		self.connection = None
		self.transactions = 0

	def is_init(self):
		return self.connection is not None

	def init(self):
		global logger
		logging.info('open lazy connection...')
		self.connection = _LasyConnection()
		self.transactions = 0

	def cleanup(self):
		self.connection.cleanup()
		self.connection = None

	def cursor(self):
		return self.connection.cursor()

#实例化
_db_ctx = _DbCtx()

engine = None


class _Engine(object):
	def __init__(self,connect):
		self._connect = connect

	def connect(self):
		return self._connect()

def create_engine(user,password,database,host='127.0.0.1',port=3306,**kwargs):
	import mysql.connector
	global engine,logger
	if engine is not None:
		raise DBError('Engine is already initialized')
	params = dict(user = user,
				  password = password,
				  database = database,
				  host = host,
				  port = port,
				  )
	defaults = dict(use_unicode = True,
					charset = 'utf8',
					collation = 'utf8_general_ci',
					autocommit = False)
	for k,v in defaults.iteritems():
		params[k] = kwargs.pop(k,v)
		params.update(kwargs)
		params['buffered'] = True
#引擎连接
		engine = _Engine(lambda: mysql.connector.connect(**params))
#hex 十六进制
		logger.info('Init mysql engine <%s> ok .'% hex(id(engine)))



class _ConnectionCtx(object):
#使用with 需要有__enter__ & __exit__ 执行进入和离开时的操作
#进入初始化engine
	def __enter__(self):
		global _db_ctx
		self.should_cleanup = False
		if not _db_ctx.is_init():
			_db_ctx.init()
			self.should_cleanup = True

#离开关闭engine
	def __exit__(self, exc_type, exc_val, exc_tb):
		global _db_ctx
		if self.should_cleanup:
			_db_ctx.cleanup()

def Connection():
	return _ConnectionCtx()

def with_connection(func):
	@functools.wraps(func)
	def _wrapper(*args,**kw):
		with _ConnectionCtx():
			return func(*args,**kw)
	return _wrapper

#with事务嵌套 ，遇到一层就+1，离开一层就-1，到0就提交事务。
class _TransactionCtx():
#with事务，初始化engine
	def __enter__(self):
		global _db_ctx,logger
		self.should_close_conn = False
		if not _db_ctx.is_init():
			_db_ctx.init()
			self.should_close_conn = True
		_db_ctx.transactions += 1
		logger.info('begin transaction...' if _db_ctx.transactions == 1 else 'Join current transaction..')
		return self

#离开，事务减1并判断是否为0，0就提交，不是就回滚。
	def __exit__(self, exc_type, exc_val, exc_tb):
		global _db_ctx
		_db_ctx.transactions -= 1
		try:
			if _db_ctx.transactions == 0 :
				if exc_type is None:
					self.commit()
				else:
					self.rollback()
		finally:
			if self.should_close_conn:
				_db_ctx.cleanup()
#提交判断，无论是否成功提交都关闭connection
	def commit(self):
		global _db_ctx,logger
		logger.info('commit transcation...')
		try:
			_db_ctx.connection.commit()
			logger.info('commit  ok.')
		except:
			logger.warning('commit failed.try rollback..')
			_db_ctx.connection.rollback()
			logger.warning('rollback ok.')
			raise
		finally:
			if self.should_close_conn:
				_db_ctx.cleanup()

#回滚
	def rollback(self):
		global _db_ctx,logger
		logger.warning('rollback transaction..')
		_db_ctx.connection.rollback()
		logger.info('rollback ok.')

def transaction():
	return _TransactionCtx()


def with_TransactionCtx(func):
	#添加执行时间记录
	@functools.wraps(func)
	def _wrapper(*args,**kw):
		start = time.time()
		with _TransactionCtx():
			_profiling(start)
	return _wrapper

#通过with_connection 进行惰性连接，查找操作
@with_connection
def _select(sql,first,*args):
	global _db_ctx,logger
	cursor = None
	sql = sql.replace('?','%s')
	logger.info('SQL:%s,ARGS:%s'%(sql,args))
	try:
		cursor = _db_ctx.connection.cursor()
		cursor.execute(sql,args)
		if cursor.description:
			names = [x[0] for x in cursor.description]
		if first:
			values = cursor.fetchone()
			if not values:
				return  None
			return Dict(names,values)
		return [Dict(names,x) for x in cursor.fetchall()]
	finally:
		if cursor:
			cursor.close()

@with_connection
def select_one(sql,*args):
	return _select(sql,True,*args)

@with_connection
def select_int(sql,*args):
	d = _select(sql,True,*args)
	if len(d) != 1:
		raise MultiColumnsError('Expect only one column.')
	return d.values()[0]

@with_connection
def select(sql,first,*args):
	return _select(sql,first,*args)

#通过with_connection 进行update数据操作。
@with_connection
def _update(sql,*args):
	global _db_ctx,logger
	cursor = None
	sql = sql.replace('?','%s')
	logger.info('SQL:%s,ARGS:%s'%(sql,args))
	try:
		cursor  = _db_ctx.connection.cursor()
		cursor.execute(sql,args)
		r = cursor.rowcount
		if _db_ctx.transactions == 0:
			logger.info('auto commit')
			_db_ctx.connection.commit()
		return r
	finally:
		if cursor:
			cursor.close()

#插入数据
def insert(table,**kw):
	cols,args = zip(*kw.iteritems())
	sql = 'insert into `%s` (%s) values (%s)' % (table, ','.join(['`%s`' % col for col in cols]), ','.join(['?' for i in range(len(cols))]))
	return _update(sql,*args)

def update(sql,*args):
	return _update(sql,*args)

if __name__ == '__main__':
	logging.basicConfig(level = logging.DEBUG)
	_logger()
	create_engine('root','Password','awesome')
	import doctest
	doctest.testmod()











