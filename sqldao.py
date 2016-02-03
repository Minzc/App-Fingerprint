import pymysql.cursors
class SqlDao:
    def __init__(self):
      try:
        self.cnx=pymysql.connect(user='zicun',password='5636595',host='127.0.0.1',db='fortinet', charset='utf8')
        self.cursor = self.cnx.cursor()
      except:
        self.cnx=pymysql.connect(user='zicun',password='5636595',host='127.0.0.1',db='fortinet', charset='utf8')
        self.cursor = self.cnx.cursor()
    
    def executeBatch(self, query, params):
        counter = 0
        for param in params:
            counter += 1
            self.cursor.execute(query, param)
            if counter == 30:
                self.cnx.commit()
                counter = 0
        return self.cursor
    
    def execute(self, query, param = None):
            if param == None:
                self.cursor.execute(query)
            else:
                self.cursor.execute(query, param)
                self.cnx.commit()
            return self.cursor
    def commit(self):
      self.cnx.commit()

    def close(self):
        self.cursor.close()
        self.cnx.commit()
        self.cnx.close()

