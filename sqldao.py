import mysql.connector
class SqlDao:
	def __init__(self):
              try:
		self.cnx=mysql.connector.connect(user='root',password='123',host='127.0.0.1',database='fortinet')
		self.cursor = self.cnx.cursor()
              except:
		self.cnx=mysql.connector.connect(user='root',password='123',host='127.0.0.1',database='fortinet')
		self.cursor = self.cnx.cursor()
        
        def executeBatch(self, query, params):
            for param in params:
                self.cursor.execute(query, param)
            self.cnx.commit()
            return self.cursor
        
	def execute(self, query, param = None):
		if param == None:
			self.cursor.execute(query)
		else:
                        # print param
			self.cursor.execute(query, param)
                        self.cnx.commit()
		return self.cursor
        def commit(self):
          self.cnx.commit()

	def close(self):
                self.cnx.commit()
		self.cnx.close()
		self.cursor.close()
