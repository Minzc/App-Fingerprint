import mysql.connector
class SqlDao:
	def __init__(self):
		self.cnx=mysql.connector.connect(user='root',password='123',host='127.0.0.1',database='fortinet')
		self.cursor = self.cnx.cursor()

	def execute(self, query, param = None):
		if param == None:
			self.cursor.execute(query)
		else:
			self.cursor.execute(query, param)
		self.cnx.commit()
		return self.cursor

	def close(self):
		self.cnx.close()
		self.cursor.close()
