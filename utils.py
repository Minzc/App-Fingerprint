#!/usr/bin/env python
# -*- coding: UTF-8 -*-
# 


# def file2mysql(file_path, pkg_name):

# 	import mysql.connector	
# 	import pyshark
	
# 	print "Start inserting", pkg_name

# 	cnx=mysql.connector.connect(user='root',password='123',host='127.0.0.1',database='fortinet')
# 	cursor = cnx.cursor()
# 	cap =pyshark.FileCapture(file_path, keep_packets = False, display_filter='http')

# 	totalIndexer = 0
	
# 	query_one = "insert into packages (appname,pgtype,time,source,desination,host, size, contenttype, path, agent, method, contentlength, httptype) VALUES (%s,%s,%s,%s,%s,%s, %s,%s,%s,%s,%s,%s,%s)"
# 	query_two = "insert into packages (appname,pgtype,time,source,desination,host, size, contenttype, path, agent, method, contentlength) VALUES (%s,%s,%s,%s,%s,%s, %s,%s,%s,%s,%s,%s)"
# 	# query_one_args = []
# 	# query_two_args = []

# 	try:
# 		package = cap.next()

# 		while package != None:
# 		# for package in cap:
# 			if 'http' in package:
# 				pkgInfo = parserPackage(package)
# 				if 'httptype' in pkgInfo:	
# 					cursor.execute(("INSERT INTO packages"
# 						"(appname,pgtype,time,source,desination,host,size, contenttype, path, agent, method, contentlength, httptype)"
# 						"VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"),
# 					 		(pkg_name, pkgInfo['type'], pkgInfo['time'],pkgInfo['src'], pkgInfo['dst'], pkgInfo['host'], pkgInfo['size'], pkgInfo['content_type'], pkgInfo['uri'], pkgInfo['agent'], pkgInfo['method'], pkgInfo['content_length'], pkgInfo['httptype']))
# 					totalIndexer += 1
# 				else:				
# 					cursor.execute("insert into packages (appname,pgtype,time,source,desination,host, size, contenttype, path, agent, method, contentlength) VALUES (%s,%s,%s,%s,%s,%s, %s,%s,%s,%s,%s,%s)",
# 					 		(pkg_name, pkgInfo['type'], pkgInfo['time'],pkgInfo['src'], pkgInfo['dst'], pkgInfo['host'], pkgInfo['size'], pkgInfo['content_type'], pkgInfo['uri'], pkgInfo['agent'], pkgInfo['method'], pkgInfo['content_length']))
# 					totalIndexer += 1
			

# 			print 'Finish Processing', totalIndexer
# 			try:
# 				package = cap.next()
# 			except Exception as inst:
# 				print "Error", pkg_name
# 				print inst
# 				package = cap.next()
			
# 	except Exception as inst:
# 		print "Error", pkg_name
# 		print inst

# 	cnx.commit()
# 	cursor.close()
# 	cnx.close()
# 	print "Finish", pkg_name, "Package:", totalIndexer


# def file2mysqlv2(file_path, pkg_name):
# 	from scapy.all import rdpcap
# 	from scapy.layers import http
# 	from scapy.all import IP
# 	import mysql.connector

# 	def parse_package(package):
		
# 		pkgInfo = {}
# 		src = package[IP].src
# 		dst = package[IP].dst
# 		time = package.time
# 		add_header = getattr(package[http.HTTP],"Additional-Headers", None)
# 		hst = getattr(package[http.HTTP],"Host", None)
# 		path = getattr(package[http.HTTP],"Path", None)
# 		accpt = getattr(package[http.HTTP],"Accept", None)
# 		agent = getattr(package[http.HTTP],"User-Agent", None) 
# 		refer = getattr(package[http.HTTP],"Referer", None) 
# 		author = getattr(package[http.HTTP],"Authorization", None) 
# 		cntlength = getattr(package[http.HTTP],"Content-Length", None) 
# 		cnttpe = getattr(package[http.HTTP],"Content-Type", None) 
# 		method = getattr(package[http.HTTP],"Method", None) 
# 		size = len(package)
# 		pkgInfo['src'] = src
# 		pkgInfo['dst'] = dst
# 		pkgInfo['time'] = time
# 		pkgInfo['add_header'] = add_header
# 		pkgInfo['hst'] = hst
# 		pkgInfo['path'] = path
# 		pkgInfo['accpt'] = accpt
# 		pkgInfo['agent'] = agent
# 		pkgInfo['refer'] = refer
# 		pkgInfo['author'] = author
# 		pkgInfo['cntlength'] = cntlength
# 		pkgInfo['cnttype'] = cnttpe
# 		pkgInfo['method'] = method
# 		pkgInfo['size'] = size
# 		if http.HTTPRequest in package:
# 			pkgInfo['httptype'] = 0
# 		elif http.HTTPResponse in package:
# 			pkgInfo['httptype'] = 1
# 		else:
# 			print package
# 			pkgInfo['httptype'] = None

# 		for k,v in pkgInfo.items():
# 			if v == None:
# 				pkgInfo[k] = ""
# 		return pkgInfo
# 	print "Start inserting", pkg_name

# 	try:
# 		caps = rdpcap(file_path)
# 		cnx=mysql.connector.connect(user='root',password='123',host='127.0.0.1',database='fortinet')
# 		cursor = cnx.cursor()
# 		query_one = ("insert into packages "
# 			"(app,src,dst,time,add_header,hst, path, accpt, agent, refer, author, cntlength, cnttype, method, size, httptype)"
# 			"VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)")
# 		httppackages=caps.filter(lambda(s): http.HTTPRequest in s or http.HTTPResponse in s)
# 		totalIndexer = 0
# 		for p in httppackages:
# 			pkgInfo = parse_package(p)

# 			cursor.execute(query_one,
# 				(pkg_name,
# 					pkgInfo['src'],
# 					pkgInfo['dst'],
# 					pkgInfo['time'],
# 					pkgInfo['add_header'], 
# 					pkgInfo['hst'],
# 					pkgInfo['path'],
# 					pkgInfo['accpt'], 
# 					pkgInfo['agent'], 
# 					pkgInfo['refer'], 
# 					pkgInfo['author'], 
# 					pkgInfo['cntlength'], 
# 					pkgInfo['cnttype'], 
# 					pkgInfo['method'],
# 					pkgInfo['size'],
# 					pkgInfo['httptype']))
# 			totalIndexer += 1

# 		cnx.commit()
# 		cursor.close()
# 		cnx.close()
# 		print "Finish", pkg_name, "Package:", totalIndexer
# 	except Exception as inst:
# 		print "Error", pkg_name
# 		print inst


# def parserPackage(package):
# 	httplayer = package['http']
# 	pkgInfo = {}

# 	pkgInfo['size'] = str(package.length)
# 	pkgInfo['type'] = 'http'
# 	pkgInfo['src'] = str(package['ip'].src)
# 	pkgInfo['dst'] = str(package['ip'].dst)

# 	pkgInfo['time'] = str(package.sniff_timestamp)
	
# 	pkgInfo['content_type'] = ''
# 	if hasattr(httplayer, 'Content-Type'):
# 		pkgInfo['content_type'] = str(getattr(httplayer, 'content_type'))

# 	pkgInfo['host'] = ''	
# 	if hasattr(httplayer, 'Host'):
# 		pkgInfo['host'] = str(getattr(httplayer, 'Host'))

# 	pkgInfo['uri'] = ''
# 	if hasattr(httplayer, 'request.uri'):
# 		pkgInfo['uri'] = str(getattr(httplayer, 'request.uri'))

# 	pkgInfo['agent'] = ''	
# 	if hasattr(httplayer, 'user_agent'):
# 		pkgInfo['agent'] = str(getattr(httplayer, 'user_agent'))

# 	pkgInfo['method'] = ''	
# 	if hasattr(httplayer, 'request.method'):
# 		pkgInfo['method'] = str(getattr(httplayer, 'request.method'))

# 	pkgInfo['content_length'] = 0
# 	if hasattr(httplayer, 'content_length'):
# 		pkgInfo['content_length'] = str(getattr(httplayer, 'content_length'))
	
# 	if hasattr(httplayer, 'request'):
# 		pkgInfo['httptype'] = 0

# 	if hasattr(httplayer, 'response'):
# 		pkgInfo['httptype'] = 1
# 	return pkgInfo


			
def loadfile(filepath, parser):
	for ln in open(filepath):
		ln = ln.strip()
		if len(ln) != 0:
			parser(ln)

# def loadcategory():
# 	"""
# 	TODO combine category with appnames
# 	"""
# 	filepath = '/Users/congzicun/Yunio/fortinet/src/source/category.txt'
# 	categories = {}
# 	def parser(ln):
# 		c, i = ln.split('\t')
# 		categories[c] = int(i)
# 	loadfile(filepath, parser)
# 	return categories

# def loadapps():
# 	"""
# 	TODO combine category with appnames
# 	"""
# 	filepath = '/Users/congzicun/Yunio/fortinet/src/source/apps.txt'
# 	apps = []
# 	def parser(ln):
# 		apps.append(ln)
# 	loadfile(filepath, parser)
# 	return apps

def multi_replace(ln, chars, new):
	for char in chars:
		ln = ln.replace(char, new)
	return ln

import tldextract
extract = tldextract.TLDExtract(
    suffix_list_url="source/effective_tld_names.dat.txt",
    cache_file=False)

def app_clean(appname):
	
	appname = appname.replace('air.','').replace('app.','').replace('android.','').replace('.android','')
	appsegs = appname.split('.')
	appname = ''
	for i in range(len(appsegs)-1,-1,-1):
		appname = appname + appsegs[i] + '.'
	appname = appname[:-1]
	extracted = extract(appname)
	if extracted.suffix != '':
		appname = appname.replace(extracted.suffix, '')
	return appname

def agent_clean(agent):
	return agent.replace('mozilla','')\
			.replace('applewebkit','')\
			.replace('chrome','')\
			.replace('safari','')\
			.replace('adobeair','')\
			.replace('apache-httpclient','')\
#inst_cat('statinfo/app.txt')
#samplepcap('/Users/congzicun/Yunio/fortinet/apk_pcaps')