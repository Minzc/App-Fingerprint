from scapy.all import rdpcap
from scapy.layers import http
from scapy.all import IP
from scapy.all import DNSRR
from scapy.all import DNS
import mysql.connector
from sqldao import SqlDao
from utils import loadfile

class ETLConsts:
	APP = 'app'
	SOURCE = 'src'
	DESTINATION = 'dst'
	TIME = 'time'
	ADD_HEADER = 'add_header'
	HOST = 'hst'
	PATH = 'path'
	ACCEPT = 'accpt'
	AGENT = 'agent'
	REFER = 'refer'
	AUTHOR = 'author'
	CONTENT_LENGTH = 'cntlength'
	CONTENT_TYPE = 'cnttype'
	METHOD = 'method'
	SIZE = 'size'
	HTTP_TYPE = 'httptype' # 0 is GET 1 is POST

class ETL:
	def __init__(self):
		self.INSERT_PACKAGES = ("INSERT INTO packages "
				"(app,src,dst,time,add_header,hst, path, accpt, agent, refer, author, cntlength, cnttype, method, size, httptype, name, category)"
				"VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)")
		self.INSERT_HOST = 'insert into host (app, host) values(%s, %s)'
		self._get_app_category()



	def _get_app_category(self):
		self.app_category = {}
		file_path = './statinfo/app.txt'
		def parser(ln):
			pkg, name, category = ln.split('\t')
			self.app_category[pkg] = (name, category)
		loadfile(file_path, parser)

	def upload_packages(self, folder):
		"""
		Insert pcap information into db
		"""
		from os import listdir
		from os.path import isfile, join

		startFlag = True
		for f in listdir(folder):
			file_path = join(folder,f)
			if isfile(file_path):	
				app_name = f[0:-5]
				self._insert_msql(join(folder,f), app_name)


	def _insert_msql(self, file_path, app_name):
		print "Start inserting", app_name
		try:
			caps = rdpcap(file_path)
			# cnx=mysql.connector.connect(user='root',password='123',host='127.0.0.1',database='fortinet')
			# cursor = cnx.cursor()
			dbdao = SqlDao()

			
			packages=caps.filter(lambda(s): http.HTTPRequest in s or http.HTTPResponse in s or DNSRR in s)
			totalIndexer = 0
			dns_info = {}
			comunicate_host = set()

			for p in packages:
				if DNSRR in p:
					self._parse_dns_package(p, dns_info)
				elif http.HTTPRequest in p or http.HTTPResponse in p:
					
					pkgInfo = self._parse_http_package(p)
					

					if pkgInfo[ETLConsts.HOST] == None or len(pkgInfo[ETLConsts.HOST]) == 0 or '.' not in pkgInfo[ETLConsts.HOST]:
						ip = pkgInfo[ETLConsts.DESTINATION]
						if ip in dns_info and len(dns_info[ip]) == 1:
							host = dns_info[ip].pop()
							pkgInfo[ETLConsts.HOST] = host
							dns_info[ip].add(host)
										

					# cursor.execute(self.INSERT_PACKAGES,
					dbdao.execute(self.INSERT_PACKAGES,
						(app_name,
							pkgInfo[ETLConsts.SOURCE],
							pkgInfo[ETLConsts.DESTINATION],
							pkgInfo[ETLConsts.TIME],
							pkgInfo[ETLConsts.ADD_HEADER], 
							pkgInfo[ETLConsts.HOST],
							pkgInfo[ETLConsts.PATH],
							pkgInfo[ETLConsts.ACCEPT], 
							pkgInfo[ETLConsts.AGENT], 
							pkgInfo[ETLConsts.REFER], 
							pkgInfo[ETLConsts.AUTHOR], 
							pkgInfo[ETLConsts.CONTENT_LENGTH], 
							pkgInfo[ETLConsts.CONTENT_TYPE], 
							pkgInfo[ETLConsts.METHOD],
							pkgInfo[ETLConsts.SIZE],
							pkgInfo[ETLConsts.HTTP_TYPE],
							self.app_category[app_name][0],
							self.app_category[app_name][1]))
					totalIndexer += 1
					comunicate_host.add(pkgInfo[ETLConsts.HOST])
				else:
					print 'ERROR WRONG PACKAGE TYPE'
			
			
			# Insert into host table
			for value in dns_info.values():
				for v in value:
					comunicate_host.add(v)

			for host in comunicate_host:
				dbdao.execute(self.INSERT_HOST, (app_name, host))
			

			dbdao.close()
			print "Finish", app_name, "Package:", totalIndexer
		except Exception as inst:
			print "Error", app_name
			print inst

	def _parse_dns_package(self, package, dns_info):
		amount = package[DNS].ancount
		for i in range(amount):
			dnsrr = package[DNSRR][i]
			host = dnsrr.rrname
			ip = dnsrr.rdata
			dns_info.setdefault(ip,set())
			dns_info[ip].add(package.qd.qname[:-1])


	def _parse_http_package(self, package):
		pkgInfo = {}
		src = package[IP].src
		dst = package[IP].dst
		time = package.time
		add_header = getattr(package[http.HTTP],"Additional-Headers", None)
		hst = getattr(package[http.HTTP],"Host", None)
		path = getattr(package[http.HTTP],"Path", None)
		accpt = getattr(package[http.HTTP],"Accept", None)
		agent = getattr(package[http.HTTP],"User-Agent", None) 
		refer = getattr(package[http.HTTP],"Referer", None) 
		author = getattr(package[http.HTTP],"Authorization", None) 
		cntlength = getattr(package[http.HTTP],"Content-Length", None) 
		cnttpe = getattr(package[http.HTTP],"Content-Type", None) 
		method = getattr(package[http.HTTP],"Method", None) 
		size = len(package)
		pkgInfo[ETLConsts.SOURCE] = src
		pkgInfo[ETLConsts.DESTINATION] = dst
		pkgInfo[ETLConsts.TIME] = time
		pkgInfo[ETLConsts.ADD_HEADER] = add_header
		pkgInfo[ETLConsts.HOST] = hst
		pkgInfo[ETLConsts.PATH] = path
		pkgInfo[ETLConsts.ACCEPT] = accpt
		pkgInfo[ETLConsts.AGENT] = agent
		pkgInfo[ETLConsts.REFER] = refer
		pkgInfo[ETLConsts.AUTHOR] = author
		pkgInfo[ETLConsts.CONTENT_LENGTH] = cntlength
		pkgInfo[ETLConsts.CONTENT_TYPE] = cnttpe
		pkgInfo[ETLConsts.METHOD] = method
		pkgInfo[ETLConsts.SIZE] = size
		if http.HTTPRequest in package:
			pkgInfo[ETLConsts.HTTP_TYPE] = 0
		elif http.HTTPResponse in package:
			pkgInfo[ETLConsts.HTTP_TYPE] = 1
		else:
			print package
			pkgInfo[ETLConsts.HTTP_TYPE] = None

		for k,v in pkgInfo.items():
			if v == None:
				pkgInfo[k] = ""
		return pkgInfo

if __name__ == '__main__':
	etl = ETL()
	etl.upload_packages('/Users/congzicun/Yunio/fortinet/apk_pcap_2000')
