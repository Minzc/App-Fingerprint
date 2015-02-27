def x_requets_classifer(package):
	if 'X-Requested-With' in package.add_header:		
		return (package.add_header.replace('X-Requested-With',''),1)
	return None

def x_unity_version(package):
	if 'X-Unity-Version' in package.add_header:		
		return (package.add_header.replace('X-Unity-Version',''),1)
	return None

def X_Umeng_Sdk(package):
	if 'X-Umeng-Sdk' in package.add_header:		
		return (package.add_header.replace('X-Umeng-Sdk',''),1)
	return None

def bundle_id(package):
	"""
	services.bearhugmedia.com
	"""
	if u'bundle_id' in package.querys:		
		return (package.querys['bundle_id'][0],2)
	return None

def app_id(package):
	"""
	mopub
	"""
	if u'app_id' in package.querys:		
		return (package.querys['app_id'][0],2)
	return None

def app_name(package):
	"""
	mopub
	googleads.g.doubleclick.net
	"""
	if u'app_name' in package.querys:		
		return (package.querys['app_name'][0],2)
	return None

def appid(package):
	"""
	s28449.ads.madgic.com
	aax-us-east.amazon-adsystem.com
	"""
	if u'appid' in package.querys:		
		return (package.querys['appid'][0],2)
	return None

def source_appstore_id(package):
	if u'source_app_store_id' in package.querys:
		return (package.querys['source_app_store_id'][0],2)
	return None

def bnd(package):
	"""
	t.manage.com
	"""
	if u'bnd' in package.querys:
		return (package.querys['bnd'][0],2)
	return None

def pkid(package):
	"""
	androidsdk.ads.mp.mydas.mobi
	"""
	if u'pkid' in package.querys:
		return (package.querys['pkid'][0],2)
	return None

def packageq(package):
	"""
	ads.mobilecore.com
	"""
	if u'package' in package.querys:
		return (package.querys['package'][0],2)
	return None

def appq(package):
	"""
	api2.playhaven.com
	"""
	if u'app' in package.querys:
		return (package.querys['app'][0],2)
	return None

def packageId(package):
	"""
	www.startappexchange.com
	"""
	if u'packageId' in package.querys:
		return (package.querys['packageId'][0],2)
	return None

def kvappn(package):
	"""
	a.adtechus.com
	"""
	if u'kvappn' in package.querys:
		return (package.querys['kvappn'][0],2)
	return None

def refer_appname(package):
	"""
	refer
	"""
	import urlparse
	querys = urlparse.parse_qs(urlparse.urlparse(package.refer).query, True)
	if u'app_name' in querys:
		return (querys['app_name'][0],3)
	return None

def refer_app(package):
	if u'app:' in package.refer:
		return (package.refer.replace('app:',''),4)
	return None


class Ruler:
	def __init__(self):
		import mysql.connector
		cnx=mysql.connector.connect(user='root',password='123',host='127.0.0.1',database='fortinet')
		cursor = cnx.cursor()
		query = 'select app, hst, agent  from test_rules'
		cursor.execute(query)
		self.hst_rules = {}
		self.hst_status = {}
		self.agent_rules = {}

		for app, hst, agent in cursor:
			if hst != None and len(hst) > 0:
				hst = hst.lower()
				self.hst_rules[hst] = app.lower()
			if agent != None and len(agent) > 0:
				self.agent_rules[agent] = app

	def classify(self, package):
		"""
		6 means over fitting
		"""

		if package.host in self.hst_rules:
			return (self.hst_rules[package.host], 5)
		if package.agent in self.agent_rules:
			return (self.agent_rules[package.agent], 6)
		return None

def classify(writedb):
	import mysql.connector
	from package import Package
	import urllib
	cnx=mysql.connector.connect(user='root',password='123',host='127.0.0.1',database='fortinet')
	cursor = cnx.cursor()
	upcursor = cnx.cursor()
	query = "select id, app, add_header,path,refer,hst,agent from packages"
	
	cursor.execute(query)
	classifiers = [x_requets_classifer,
					x_unity_version,
					bundle_id, 
					app_id, 
					app_name, 
					appid, 
					refer_appname, 
					source_appstore_id,
					bnd,
					packageq,
					appq,
					packageId,
					refer_appname,
					refer_app]
	clsf_rst = {}
	regapps = set()
	rule = Ruler()

	for id, app, add_header, path, refer, host, agent in cursor:
		package = Package()
		package.set_path(path)
		package.set_add_header(add_header)
		package.set_refer(refer)
		package.set_host(host)
		package.set_agent(agent)
		# print package.querys
		for classifier in classifiers:
			identifier = classifier(package)
			if identifier != None and identifier != '':
				regapps.add(app)
				if writedb:
					clsf_rst[id] = identifier[1]
				
		if id not in clsf_rst:
			rst = rule.classify(package)
			if rst is not None:
				clsf_rst[id] = rst[1]


	
	upquery = "update packages set classified = %s where id = %s"	
	for k,v in clsf_rst.items():
		upcursor.execute(upquery, (v, k))
	cnx.commit()
	print "Number of recognized packages:", len(clsf_rst)

if __name__ == '__main__':
	classify(True)