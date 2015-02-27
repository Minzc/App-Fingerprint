from nltk import FreqDist
from utils import loadfile
from utils import loadcategory
from utils import tpdomain
import sys

def stat_hstNapp(filepath):
	counter = FreqDist()
	def parser(ln):
		if len(ln.split('\t')) < 2:
			return
		app, host, time = ln.split('\t')
		# remove port
		colon_pos = host.find(':')
		if colon_pos != -1:
			host = host[0:colon_pos]
		host_segs = host.split('.')
		# only keep the top domain
		if len(host_segs) >= 2:
			host = host_segs[-2]+'.'+host_segs[-1]
			counter.inc(host, int(time))
	loadfile(filepath, parser)
	for k,v in counter.items():
		print "%s\t%s" % (k,v)
def stat_catNapp(filepath):

	categories = loadcategory()
	chart = {}
	def parser(ln):
		if len(ln.split('\t')) < 2:
			return
		cat, host, time = ln.split('\t')
		# only keep the top domain
		
		host = tpdomain(host)
		if host not in chart:
			chart[host] = [0]*len(categories)
		chart[host][categories[cat]] += 1
	loadfile(filepath, parser)
	for k,v in chart.items():
		sys.stdout.write(k)
		counter = 0
		for i in range(len(categories)):
			if(v[i]!=0):
				counter+=1
			sys.stdout.write('\t'+str(v[i]))
		sys.stdout.write('\t'+str(counter))
		print			

def statUrlToken(filepath):
	import urllib
	"""
	find out urls contain com
	"""
	def parser(ln):
		ln = ln.lower()
		if len(ln.split(' ')) == 1:
			return
		app, path = ln.split(' ')
		path = urllib.unquote(path)
		path_segs = path.split('&')
		
		for path_seg in path_segs:			
			if 'com.' in path_seg and '=' in path_seg:
				parameter = path_seg.split('=')[0]
				print "%s\t%s\t%s" % (parameter, path_seg, app)
	loadfile(filepath, parser)

def stat_relation(filepath, col1, col2, outfile):
	"""
	Number of times col1 and col2 co-occure
	"""
	from pandas import DataFrame
	from pandas import Series
	from nltk import FreqDist
	from utils import loadfile
	from utils import loadapps
	writer = open(outfile, 'w')
	def tofile(writer, row_counter, row_indx):
		ln = row_indx.strip().replace(',',' ')
		for app in apps:
			if app in row_counter:
				ln = ln + ',' + str(row_counter[app])
			else:
				ln = ln + ',0'
		writer.write(ln+'\n')

	apps = loadapps()
	header = ''
	for app in apps:
		header = header + ',' + app
	writer.write(header+'\n')

	row_counter = None
	row_indx = None
	counter = 0
	for ln in open(filepath):
		lnsegs = ln.strip().split('\t')
		if len(lnsegs) > col2 and lnsegs[col2] != '':
			counter += 1
			if row_indx != lnsegs[col2]:				
				if row_indx != None:
					tofile(writer, row_counter, row_indx)
				row_indx = lnsegs[col2]
				row_counter = FreqDist()

			row_counter.inc(lnsegs[col1])
			print 'Finish Processing',counter
	# row = Series(row_counter)
	tofile(writer, row_counter, row_indx)
	writer.close()

def host_freq_mine(filepath):
	"""
	segment host and count number of parts
	"""
	from nltk import FreqDist
	from utils import multi_replace
	counter = FreqDist()
	def parser(ln):
		ln = multi_replace(ln, ['.','_','-'], '')
		lnsegs = ln.split(' ')
		for lnseg in lnsegs:
			counter.inc(lnseg)
	for k,v in counter.items():
		print "%s\t%s" % (k,v)

def hst_clst_id(filepath):
	"""
	get hosts that occurr in only one cluster
	"""
	filecontent = []
	def parser(ln):
		filecontent.append(ln)
	hst_app = {}
	indx = 0
	loadfile(filepath,parser)
	for ln in filecontent:
		hosts, apps = ln.split('\t')
		for host in hosts.split(','):
			if host not in hst_app:
				hst_app[host] = set()
			hst_app[host].add(indx)
		indx += 1
	candidate_host = {}
	for k,v in hst_app.items():
		if len(v) == 1:
			n = v.pop()
			if n not in candidate_host:
				candidate_host[n] = set()
			candidate_host[n].add(k)
	
	for k,v in candidate_host.items():
		if len(v) == 1:
			print "%s\t$$$$$$%s" % ('\t'.join(v),filecontent[k])

def hst_n_secdomain():
	"""
	hst secdomain:app
	"""
	import mysql.connector
	from package import Package
	cnx=mysql.connector.connect(user='root',password='123',host='127.0.0.1',database='fortinet')
	cursor = cnx.cursor()
	query = 'select app, hst from packages'
	cursor.execute(query)
	hst = {}
	for app, host in cursor:
		package = Package()
		package.set_host(host)
		host  = package.host
		secdomain = package.secdomain
		
		if secdomain != None and len(host) > len(secdomain):
			if secdomain not in hst:
				hst[secdomain] = set()	
			hst[secdomain].add(app+':'+host)
	for k,v in hst.items():
		print "%s\t%s" % (k, '\t'.join(v))
		print
def adserverNkey():
	"""
	ad service ,app, key
	"""
	import mysql.connector
	from package import Package
	from nltk import FreqDist

	cnx=mysql.connector.connect(user='root',password='123',host='127.0.0.1',database='fortinet')
	cursor = cnx.cursor()
	query = 'select app, path, hst from packages'

	apphst = FreqDist()
	apphstkeys = FreqDist()
	cursor.execute(query)
	for app, path, hst in cursor:
		package = Package()
		package.set_host(hst)
		package.set_path(path)
		for k,v in package.querys.items():
			id = app + '##' + hst
			id2 = id + '##' + v[0]
			if len(v[0]) > 1:
				apphst.inc(id)
				apphstkeys.inc(id2)

	for k,v in apphstkeys.items():
		
		app, hst, token = k.split('##')
		if v == apphst[app+'##'+hst]:
			print k

def findAppNHost():
	"""
	Find same parts in apps and hosts
	"""
	import mysql.connector
	from package import Package
	from nltk import FreqDist

	cnx=mysql.connector.connect(user='root',password='123',host='127.0.0.1',database='fortinet')
	cursor = cnx.cursor()
	query = 'select app, hst from packages group by app, hst'
	cursor.execute(query)

	for appname, hst in cursor:
		app = appname.replace('air.','').replace('com.','').replace('br.','').replace('net.','').replace('au.','').replace('ca.','').replace('cn.','').replace('co.','')
		appsegs = app.split('.')
		hstsegs = set(hst.split('.'))
		for appseg in appsegs:
			if appseg in hstsegs:
				print appname, hst
		


findAppNHost()
#adserverNkey()
#hst_n_secdomain()
#hst_clst_id('/Users/congzicun/Yunio/fortinet/src/host_cluster.txt')
#stat_relation('/Users/congzicun/Yunio/fortinet/src/appNtokens.txt', 0, 2, 'statToken.csv')
#statUrlToken('/Users/congzicun/Yunio/fortinet/src/urltmp.csv')
# stat_hstNapp('categoryNhost.txt')
#stat_catNapp('categoryNhost.txt')