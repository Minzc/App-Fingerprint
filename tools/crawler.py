from sqldao import SqlDao
import urllib2
import re
def crawl_add_info():
	app_cmp = {}
	app_dev = {}
	appnames = {}
	for app in open('/Users/congzicun/Yunio/fortinet/src/statinfo/app.txt'):
		appnames[app.split('\t')[0]] = ''

	QUERY_ONE = 'UPDATE apps SET company = %s WHERE app = %s'
	QUERY_TWO = 'UPDATE apps SET dev = %s WHERE app = %s'

	sqldao = SqlDao()

	for app in appnames.keys():
		try:
			if len(appnames[app]) == 0:
				url = 'https://play.google.com/store/apps/details?id=%s&hl=en' % (app)
				print url
				response = urllib2.urlopen(url)
				content = response.read()
				r = re.search('<span itemprop="name">(.*?)</span>', content)
				if r:
					company = r.group(1)
					rst = sqldao.execute(QUERY_ONE, (company, app))
					print 'success company', rst

				else:
					print 'Failed company'
				r = re.search('"mailto:(.*?)"', content)
				if r:
					dev = r.group(1)
					rst = sqldao.execute(QUERY_TWO, (dev, app))
					print 'success mail',rst
				else:
					print 'Failed mail'
		except:
			print 'Error'
	

crawl_add_info()
