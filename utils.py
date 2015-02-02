#!/usr/bin/env python
# -*- coding: UTF-8 -*-
# 
def file2mysql(file_path, pkg_name):
	import mysql.connector	
	import pyshark
	
	print "Start inserting", pkg_name

	cnx=mysql.connector.connect(user='root',password='123',host='127.0.0.1',database='fortinet')
	cursor = cnx.cursor()
	cap =pyshark.FileCapture(file_path)

	counter = 0
	
	try:
		for package in cap:
			if 'http' in package:
				pkgInfo = parserPackage(package)
				if 'httptype' in pkgInfo:				
					cursor.execute("insert into Packages (appname,pgtype,time,source,desination,host,  size, contenttype, path, agent, method, contentlength, httptype) VALUES (%s,%s,%s,%s,%s,%s, %s,%s,%s,%s,%s,%s, %s)",
				 		(pkg_name, pkgInfo['type'], pkgInfo['time'],pkgInfo['src'], pkgInfo['dst'], pkgInfo['host'], pkgInfo['size'], pkgInfo['content_type'], pkgInfo['uri'], pkgInfo['agent'], pkgInfo['method'], pkgInfo['content_length'], pkgInfo['httptype']))
					counter += 1
				else:				
					cursor.execute("insert into Packages (appname,pgtype,time,source,desination,host, size, contenttype, path, agent, method, contentlength, httptype) VALUES (%s,%s,%s,%s,%s,%s, %s,%s,%s,%s,%s,%s, %s)",
				 		(pkg_name, pkgInfo['type'], pkgInfo['time'],pkgInfo['src'], pkgInfo['dst'], pkgInfo['host'], pkgInfo['size'], pkgInfo['content_type'], pkgInfo['uri'], pkgInfo['agent'], pkgInfo['method'], pkgInfo['content_length']))
					counter += 1
		cnx.commit()
	except:
		print "Error", pkg_name

	cursor.close()
	cnx.close()
	print "Finish", pkg_name, "Package:", counter

def parserPackage(package):
	httplayer = package['http']
	
	pkgInfo = {}

	pkgInfo['size'] = package.length
	pkgInfo['type'] = 'http'
	pkgInfo['src'] = package['ip'].src
	pkgInfo['dst'] = package['ip'].dst

	pkgInfo['time'] = package.sniff_timestamp
	
	pkgInfo['content_type'] = ''
	if hasattr(httplayer, 'Content-Type'):
		pkgInfo['content_type'] = getattr(httplayer, 'content_type')

	pkgInfo['host'] = ''	
	if hasattr(httplayer, 'Host'):
		pkgInfo['host'] = getattr(httplayer, 'Host')

	pkgInfo['uri'] = ''
	if hasattr(httplayer, 'request.uri'):
		pkgInfo['uri'] = getattr(httplayer, 'request.uri')

	pkgInfo['agent'] = ''	
	if hasattr(httplayer, 'user_agent'):
		pkgInfo['agent'] = getattr(httplayer, 'user_agent')

	pkgInfo['method'] = ''	
	if hasattr(httplayer, 'request.method'):
		pkgInfo['method'] = getattr(httplayer, 'request.method')

	pkgInfo['content_length'] = ''
	if hasattr(httplayer, 'content_length'):
		pkgInfo['content_length'] = getattr(httplayer, 'content_length')
	
	if hasattr(httplayer, 'request'):
		pkgInfo['httptype'] = 0

	if hasattr(httplayer, 'response'):
		pkgInfo['httptype'] = 1
	return pkgInfo

#file2mysql('/Users/congzicun/Yunio/fortinet/apk_pcaps/ca.indigo.pcap','ca.indigo.pcap')

def transportfiles(mypath):
	from os import listdir
	from os.path import isfile, join

	startFlag = False
	for f in listdir(mypath):
		if isfile(join(mypath,f)):
			if f == 'air.cn.kx.yttxgoogleTwbattle.pcap':
				startFlag = True
			if startFlag:
				file2mysql(join(mypath,f), f[0:-5])
			

transportfiles('/Users/congzicun/Yunio/fortinet/apk_pcaps')

