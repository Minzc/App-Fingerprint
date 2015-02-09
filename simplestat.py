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
	from pandas import DataFrame
	from pandas import Series
	from nltk import FreqDist
	from utils import loadfile
	from utils import loadapps
	writer = open(outfile, 'w')
	def tofile(writer, row_counter, row_indx):
		ln = row_indx
		for app in apps:
			if app in row_counter:
				ln = ln + ',' + str(row_counter[app])
			else:
				ln = ln + ',0'
		writer.write(ln+'\n')

	apps = loadapps()
	
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
	tofile(writer, row_counter, row_counter)
	writer.close()





stat_relation('/Users/congzicun/Yunio/fortinet/src/sorted_appNtokens.txt', 0, 2, 'statToken.csv_cmp')
#statUrlToken('/Users/congzicun/Yunio/fortinet/src/urltmp.csv')
# stat_hstNapp('categoryNhost.txt')
#stat_catNapp('categoryNhost.txt')