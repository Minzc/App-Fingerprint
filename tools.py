# -*- utf-8 -*-

import sys
from sqldao import SqlDao
from utils import Relation
import math
from utils import load_pkgs
from package import Package
from nltk import FreqDist

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


def tf_idf(data_set = None):
	sqldao = SqlDao()
	if not data_set:
		data_set = []
		QUERY = 'SELECT path, app FROM packages'
		for path, app in sqldao.execute(QUERY):
			package = Package()
			package.set_path(path)
			package.set_app(app)

	pthsegApp = Relation()
	appCounter = set()
	for package in data_set:
		appCounter.add(package.app)
		for pathseg in package.path.split('/'):
			if len(pathseg) > 0:
				pthsegApp.add(pathseg, package.app)

	totalDoc = len(appCounter)
	INSERT = 'INSERT INTO tfidf (word, tfidf, app) VALUES (%s, %s, %s)'
	for pathseg, counter in pthsegApp.get().items():
		doc = len(counter)
		for app, count in counter.items():
			sqldao.execute(INSERT, (pathseg, math.log(1.0 * totalDoc / doc) * count, app))
	sqldao.close()

def gen_cmar_data(limit = None):
	
	records = load_pkgs(limit)
	feature_index = {}
	app_index = {}
	train_data = []
	f_indx = 0
	f_counter = FreqDist()
	f_company = Relation()

	for record in records:
		pathsegs = filter(None, record.path.split('/'))
		recordVec = []
		for pathseg in pathsegs:
			f_counter.inc(pathseg)
			f_company.add(pathseg, record.company)
	valid_f = set()

	for k, v in f_counter.items():
		if v > 1 and len(f_company.get()[k]) < 4:
			valid_f.add(k)

	
	for record in records:
		pathsegs = filter(None, record.path.split('/'))
		recordVec = []
		for pathseg in pathsegs:
			if pathseg not in valid_f:
				continue

			if pathseg not in feature_index:
				f_indx += 1
				feature_index[pathseg] = f_indx
			f_counter.inc(feature_index[pathseg])
			recordVec.append(feature_index[pathseg])
			f_company.add(feature_index[pathseg], record.company)
		train_data.append((record.app, recordVec))

	fwriter = open('record_vec.txt', 'w')
	# train_data
	# (app, [f1, f2, f3])
	
	for record in train_data:
		outstr = ''

		for f in sorted({i for i in record[1] if f_counter[i] > 1 and len(f_company.get()[i]) < 4}, reverse = True):
			outstr = str(f) + ' ' + outstr
		if outstr:
			if record[0] not in app_index:
				f_indx += 1
				app_index[record[0]] = f_indx
			outstr += str(app_index[record[0]])
			fwriter.write(outstr+'\n')
	fwriter.close()
	fwriter = open('app_index.txt', 'w')
	for k, v in app_index.items():
		fwriter.write(k+'\t'+str(v)+'\n')
	fwriter.close()
	fwriter = open('feature_index.txt', 'w')
	for k, v in feature_index.items():
		fwriter.write(k.encode('utf-8')+'\t'+str(v)+'\n')
	fwriter.close()
	print 'output files are app_index.txt, feature_index.txt, record_vec.txt'
	print 'number of classes:', len(app_index)
				

if __name__ == '__main__':
	
	if len(sys.argv) < 2:
		print 'error'	
	elif sys.argv[1] == 'geturl':
		queryUrlToken(True)
	elif sys.argv[1] == 'trans':
		transportfiles('/Users/congzicun/Yunio/fortinet/apk_pcaps')
	elif sys.argv[1] == 'uniquehst':
		select_uniq_hst()
	elif sys.argv[1] == 'trans_dns':
		transport_dns_info('/Users/congzicun/Yunio/fortinet/apk_pcaps')
	elif sys.argv[1] == 'gen_record_vec':
		if len(sys.argv) == 3:
			gen_cmar_data(sys.argv[2])
		else:
			gen_cmar_data()





####################################################
# def insert_rules(filepath):
# 	import mysql.connector
# 	from utils import loadfile
# 	cnx=mysql.connector.connect(user='root',password='123',host='127.0.0.1',database='fortinet')
# 	cursor = cnx.cursor()
# 	query = 'insert into rules (app, hst, status) values (%s, %s, %s)'
# 	def parser(ln):
# 		hst, _, apps = ln.split('\t')
# 		status, hst = hst.split(' ')
# 		for app in apps.split(','):
# 			cursor.execute(query, (app, hst, status))
# 	loadfile(filepath, parser)
# 	cnx.commit()	


####################################################
# def samplepcap(file_path):
# 	from os import listdir
# 	from os.path import isfile, join
# 	import pyshark

# 	startFlag = True
# 	for f in listdir(file_path):
# 		if isfile(join(file_path,f)):
# 			cap =pyshark.FileCapture(join(file_path,f), keep_packets = False, display_filter='http')
# 			try:
# 				for p in cap:
# 					print f
# 					print p['http']
# 					print '-'*10
# 			except:
# 				pass

####################################################
# def extractHttpHeads(file_path):
# 	from os import listdir
# 	from os.path import isfile, join
# 	import pyshark
# 	startFlag = True
# 	for f in listdir(file_path):
# 		if isfile(join(file_path,f)):
# 			cap =pyshark.FileCapture(join(file_path,f), keep_packets = False, display_filter='http')
# 			try:
# 				for p in cap:
# 					print f
# 					print p['http']
# 					print '-'*10
# 			except:
# 				pass

####################################################
#def transportfiles(mypath):
# 	"""
# 	Insert pcap information into db
# 	"""
# 	from os import listdir
# 	from os.path import isfile, join
# 	from utils import file2mysqlv2
# 	startFlag = True
# 	for f in listdir(mypath):
# 		if isfile(join(mypath,f)):	
# 			file2mysqlv2(join(mypath,f), f[0:-5])
