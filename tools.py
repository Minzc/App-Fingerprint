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
	"""
	Insert pcap information into db
	"""
	from os import listdir
	from os.path import isfile, join
	from utils import file2mysqlv2

	startFlag = True
	for f in listdir(mypath):
		if isfile(join(mypath,f)):	
			file2mysqlv2(join(mypath,f), f[0:-5])


def transport_dns_info(mypath):
	"""
	Insert pcap information into db
	"""
	from os import listdir
	from os.path import isfile, join
	from scapy.all import rdpcap
	from scapy.layers import http
	from scapy.all import IP
	from scapy.all import DNSRR
	from scapy.all import DNS
	from sqldao import SqlDao
	startFlag = True
	sqldao = SqlDao()
	query = 'insert into host (app, host) values(%s, %s)'
	for f in listdir(mypath):
		if isfile(join(mypath,f)):
			try:
				file_path = join(mypath,f)
				appname = f[0:-5]
				caps = rdpcap(file_path)
				dnspackages=caps.filter(lambda(s): DNSRR in s)
				hosts = set()
				for dnspackage in dnspackages:
					amount = dnspackage[DNS].ancount
					for i in range(amount):
						hosts.add(dnspackage[4].qname[:-1])
				for host in hosts:
					sqldao.execute(query, (appname, host))
					print host
			except Exception as inst:
				print "Error", appname
				print inst
			
	sqldao.close()




def queryUrlToken():
	import urllib
	import mysql.connector
	from package import Package
	cnx=mysql.connector.connect(user='root',password='123',host='127.0.0.1',database='fortinet')
	cursor = cnx.cursor()
	query = "select app, path from packages where httptype = 0"
	cursor.execute(query)
	for app, path in cursor:
		package = Package()
		package.set_path(path)
		for k, v in package.querys.items():
			print "%s\t%s\t%s" % (app.encode('utf-8'),k.encode('utf-8'), v[0].encode('utf-8'))

def queryUrlToken(both):
	import urllib
	import mysql.connector
	from package import Package
	cnx=mysql.connector.connect(user='root',password='123',host='127.0.0.1',database='fortinet')
	cursor = cnx.cursor()
	query = "select app, path from packages where httptype = 0"
	cursor.execute(query)
	for app, path in cursor:
		package = Package()
		package.set_path(path)
		outln = app
		if both:
			for k, v in package.querys.items():
				print "%s\t%s\t%s" % (app.encode('utf-8'),k.encode('utf-8'), v[0].encode('utf-8'))
		else:
			for k,v in package.querys.items():
				outln = outln + '\t' + k
			if len(package.querys) > 0:
				print outln.encode('utf-8')

def select_uniq_hst():
	from nltk import FreqDist
	import mysql.connector
	counter = FreqDist()
	cnx=mysql.connector.connect(user='root',password='123',host='127.0.0.1',database='fortinet')
	cursor = cnx.cursor()
	query = 'select app, hst from packages where httptype = 0'
	cursor.execute(query)
	pairs = set()
	for app, host in cursor:
		if app + "$" + host not in pairs:
			pairs.add(app + "$" + host)
			counter.inc(host)
		
	for k,v in counter.items():
		print "%s\t%s" % (k,v)

def get_app_token():
	import mysql.connector
	cnx=mysql.connector.connect(user='root',password='123',host='127.0.0.1',database='fortinet')
	cursor = cnx.cursor()

def insert_rules(filepath):
	import mysql.connector
	from utils import loadfile
	cnx=mysql.connector.connect(user='root',password='123',host='127.0.0.1',database='fortinet')
	cursor = cnx.cursor()
	query = 'insert into rules (app, hst, status) values (%s, %s, %s)'
	def parser(ln):
		hst, _, apps = ln.split('\t')
		status, hst = hst.split(' ')
		for app in apps.split(','):
			cursor.execute(query, (app, hst, status))
	loadfile(filepath, parser)
	cnx.commit()	

if __name__ == '__main__':
	
	if len(sys.argv) < 2:
		print 'error'	
	elif sys.argv[1] == 'geturl':
		queryUrlToken(True)
	elif sys.argv[1] == 'trans':
		transportfiles('/Users/congzicun/Yunio/fortinet/apk_pcaps')
	elif sys.argv[1] == 'uniquehst':
		select_uniq_hst()
	elif sys.argv[1] == 'instrules':
		insert_rules('/Users/congzicun/Yunio/fortinet/src/a')
	elif sys.argv[1] == 'trans_dns':
		transport_dns_info('/Users/congzicun/Yunio/fortinet/apk_pcaps')