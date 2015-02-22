from package import Package
from sqldao import SqlDao
from utils import app_clean
def clusterbyhost():
	import mysql.connector
	from package import Package
	import urllib
	import tldextract
	cnx=mysql.connector.connect(user='root',password='123',host='127.0.0.1',database='fortinet')
	cursor = cnx.cursor()
	upcursor = cnx.cursor()
	query = "select id, app, hst, dst from packages where httptype=0"
	
	cursor.execute(query)

	app_hst = {}
	app_dst = {}
	for id, app, hst,dst in cursor:
		extracted = tldextract.extract(hst)
		hst = "{}.{}".format(extracted.domain, extracted.suffix)
		if len(hst) > 2:
			if app not in app_hst:
				app_hst[app] = set()

			if app not in app_dst:
				app_dst[app] = set()	

			if len(hst) > 0:
				app_hst[app].add(hst)

			
			app_dst[app].add(dst)

	hst_app = {}
	dst_app = {}

	for app, hst_lst in app_hst.items():
		hst_ln = ','.join(hst_lst)
		if hst_ln not in hst_app:
			hst_app[hst_ln] = set()
		hst_app[hst_ln].add(app)

	for app, dst_lst in app_dst.items():
		dst_ln = ','.join(dst_lst)
		if dst_ln not in dst_app:
			dst_app[dst_ln] = set()
		dst_app[dst_ln].add(app)

	for k,v in hst_app.items():
		# if len(v) > 1:
		print "%s\t%s" % (k.encode('utf-8'), ','.join(v).encode('utf-8'))
		print
"""
	for k,v in dst_app.items():
		if len(v) > 1:
			print "%s\t%s" % (k.encode('utf-8'), ','.join(v).encode('utf-8'))
			print
"""


class ClusterHost:
	import urllib
	import tldextract

	def __init__(self):
		self.app_host = {}

	def process(self, package):
		app = package.app
		host = package.host
		secdomain = package.secdomain
		self.app_host.setdefault(app, set())
		if secdomain != None:
			self.app_host[app].add(host) # MARK

	def result(self, contrains):
		"""
		app1,app2 \t {hst1, hst2, hst3}
		"""
		
		host_app = {}
		for app, hostlst in self.app_host.items():
			hostlstln = ','.join(hostlst)
			host_app.setdefault(hostlstln, set())
			host_app[hostlstln].add(app)
			
		indx = 0
		indx_clusters = []
		indx_apps = {} # app : clusterid
		for hostlstln, apps in host_app.items():
			for app in apps:
				indx_apps[app] = indx
			indx += 1
			indx_clusters.append((apps, set(hostlstln.split(','))))

		
		for company, apps in contrains.items():
			newapps = set()
			newhostlist = set()
			for app in apps:
				# print indx_clusters[indx_apps[app]], indx_apps[app], len(indx_clusters)
				if indx_clusters[indx_apps[app]] != None:
					newapps |= indx_clusters[indx_apps[app]][0]
					newhostlist |= indx_clusters[indx_apps[app]][1]
				indx_clusters[indx_apps[app]] = None
				indx_apps[app] = len(indx_clusters)
			if len(newapps) > 0:
				indx_clusters.append((newapps, newhostlist))

		rst = {}
		for i in range(len(indx_clusters)):
			if indx_clusters[i] != None:
				apps, hostlsts = indx_clusters[i]
				rst[','.join(apps)] = ','.join(hostlsts)
				# print ','.join(apps), "$$$$$", ','.join(hostlsts)
				# print
		return rst
		


class ClusterPkgName:
	
	def __init__(self):
		self.company = {}

	def process(self, package):
		companyname = app_clean(package.app).split('.')[0]
		self.company.setdefault(companyname, set())
		self.company[companyname].add(package.app)

	def result(self):
		"""
		company \t {app1, app2}
		"""
		return self.company


#clusterbyhost()