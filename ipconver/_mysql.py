# coding=UTF-8

import pymysql
from DBUtils.PooledDB import PooledDB

device_pool = PooledDB(
	pymysql,
	1,
	host="10.0.12.238",
	port=3333, 
	user="root",
	passwd="rootroot",
	db='sale_stat',
	charset="utf8",
)
bs_rom_data_pool= PooledDB(
	pymysql,
	1,
	host='111.230.128.101',
	port = 3317,
	user='root',
	passwd='saleroot',
	charset="utf8",
	db ='sale_stat',
)

package_pool= PooledDB(
	pymysql,
	1,
	host='10.0.12.207',
	port = 3310,
	user='rom_mysql',
	passwd='romrommysql4521',
	charset="utf8",
	db ='bs_rom_analy_data',
)
# online_pool = PooledDB(
# 	pymysql,
# 	1,
# 	host="10.10.1.11",
# 	port=3320, 
# 	user="account_dev",
# 	passwd="account_dev",
# 	db='device_server',
# 	charset="utf8",
# )

SQL_DB_POOL_MAP = {
	# "online_server":online_pool,
	"device_server": device_pool,
	"package_server":package_pool,
	"bs_rom_data_server":bs_rom_data_pool,
}

class OperateSQL:
	def __init__(self, database):
		self.db = database

	def connectDB(self):
		if self.db in SQL_DB_POOL_MAP:
			self._db = SQL_DB_POOL_MAP[self.db].connection()
		else:
			print("999999")
		
	def query_get(self, sql):
		try:
			self.connectDB()
			cursor = self._db.cursor(pymysql.cursors.DictCursor)
			cursor.execute('SET NAMES utf8')
			cursor.execute(sql)
			result = cursor.fetchall()
			
			cursor.close()
			self.close()
			return True, result
		except pymysql.Error as e:
			print(e)
			try:
				self.close()
			except Exception as ex:
				print(ex)
			return False, e.args[0]
		
	def query_update(self, sql):
		try:
			self.connectDB()
			cursor = self._db.cursor()
			cursor.execute('SET NAMES utf8')
			print(sql)
			cursor.execute(sql)
			self._db.commit()
			cursor.close()
			self.close()
			return True, ""
		except pymysql.Error as e:
			print(e)
			try:
				self.close()
			except Exception as ex:
				print("execute query error : ", str(ex))
			return False, e.args[0]
	def insert(self, query, params):
		try:
			self.connectDB()
			cursor = self._db.cursor()
			cursor.execute('SET NAMES utf8')
			cursor.execute(query, params)
			self._db.commit()
			cursor.close()
			self.close()
			return True, ""
		except Exception as e:
			print(e)
			self._db.rollback()
			
	def close(self):
		self._db.close()
				

if __name__ == '__main__':
	pass
	
	"""
	optSQL = OperateSQL()
	
	columns = "StationKey,CellLocation,PhoneType,Time,IMEI,Module,Version,id,OperatorNumeric"
	chartname = conf.G_SQL_CHARTNAME
	conditiondict = {
		"checkState": "SUCC"
	}
	conOper = "="
	selectedData = optSQL.read_sql(columns,chartname,conditiondict,conOper)
		
	for row in selectedData:
		for oneItem in row:
			print str(oneItem)
	"""
	