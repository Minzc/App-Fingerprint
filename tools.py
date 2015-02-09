# -*- utf-8 -*-

import sys
def inst_cat(file_path):
	"""
	Insert app's category into db
	"""
	appNcats = {}	
	def parser(ln):
		pkgname, _, category = ln.split('\t')
		category = category.replace(' ', '').lower()
		appNcats[pkgname] = category

	loadfile(file_path, parser)
	import mysql.connector	
	cnx=mysql.connector.connect(user='root',password='123',host='127.0.0.1',database='fortinet')
	cursor = cnx.cursor()
	query = 'update packages set category=%s where appname=%s'
	for k,v in appNcats.items():
		print v, k
		cursor.execute(query, (v, k))
	cnx.commit()
	cursor.close()
	cnx.close()

def samplepcap(file_path):
	from os import listdir
	from os.path import isfile, join
	import pyshark

	startFlag = True
	for f in listdir(file_path):
		if isfile(join(file_path,f)):
			cap =pyshark.FileCapture(join(file_path,f), keep_packets = False, display_filter='http')
			try:
				for p in cap:
					print f
					print p['http']
					print '-'*10
			except:
				pass

def extractHttpHeads(file_path):
	from os import listdir
	from os.path import isfile, join
	import pyshark

	startFlag = True
	for f in listdir(file_path):
		if isfile(join(file_path,f)):
			cap =pyshark.FileCapture(join(file_path,f), keep_packets = False, display_filter='http')
			try:
				for p in cap:
					print f
					print p['http']
					print '-'*10
			except:
				pass

def transportfiles(mypath):
	from os import listdir
	from os.path import isfile, join
	from utils import file2mysqlv2

	startFlag = True
	for f in listdir(mypath):
		if isfile(join(mypath,f)):	
			file2mysqlv2(join(mypath,f), f[0:-5])
def queryUrlToken():
	import urllib
	import mysql.connector
	cnx=mysql.connector.connect(user='root',password='123',host='127.0.0.1',database='fortinet')
	cursor = cnx.cursor()
	query = "select app, path from packages_bak"
	cursor.execute(query)
	for app, path in cursor:
		import urlparse
		querys = urlparse.parse_qs(urlparse.urlparse(path).query, True)
		for k, v in querys.items():
			print "%s\t%s\t%s" % (app.encode('utf-8'),k.encode('utf-8'), v[0].encode('utf-8'))

if __name__ == '__main__':
	
	if len(sys.argv) < 2:
		print 'error'	
	elif sys.argv[1] == 'geturl':
		queryUrlToken()