from package import Package
from sqldao import SqlDao
from utils import load_pkgs

def header_classifier(package):
    identifier = ['x-umeng-sdk', 'x-vungle-bundle-id', 'x-requested-with']
    for id in identifier:
        for head_seg in package.add_header.split('\n'):
            if id in head_seg:
                return (head_seg.replace(id + ':', '').strip(), 1)
    return None


def agent_classifier(package):
    if package.app in package.agent:
        return (package.app, 2)
    return None


# class Ruler:
# def __init__(self):
# 		import mysql.connector
# 		cnx=mysql.connector.connect(user='root',password='123',host='127.0.0.1',database='fortinet')
# 		cursor = cnx.cursor()
# 		query = 'select app, hst, company, agent, path from rules'
# 		cursor.execute(query)
# 		self.hst_rules = {}
# 		self.agent_rules = {}
# 		self.company_rules = {}
# 		self.path_rules = {}

# 		for app, hst, company, agent, path in cursor:
# 			hst = hst.split(':')[0].replace('www.','')
# 			app, hst, company, agent, path = lower_all((app, hst, company, agent, path))
# 			if path and len(path) > 0:
# 				self.path_rules.setdefault(hst, {})
# 				if app:
# 					self.path_rules[hst][path] = app
# 				else:
# 					self.path_rules[hst][path] = company
# 				self.company_rules[hst] = company
# 			elif app and len(app) > 0:
# 				self.hst_rules[hst.lower()] = app
# 			elif company and len(company) > 0:
# 				self.company_rules[hst] = company


# 	def classify(self, package):
# 		"""
# 		6 means over fitting
# 		"""
# 		# print package.host in self.hst_rules, package.host


# 		if package.host in self.hst_rules:
# 			return (self.hst_rules[package.host], 5)

# 		# elif package.agent in self.agent_rules:
# 		# 	return (self.agent_rules[package.agent], 6)
# 		elif package.host in self.path_rules:
# 			pathsegs = package.path.split('/')
# 			for pathseg in pathsegs:
# 				if pathseg in self.path_rules[package.host]:
# 					return (self.path_rules[package.host][pathseg], 8)

# 		if package.host in self.company_rules:
# 			return (self.company_rules[package.host], 9)

# 		return None

def classify(writedb, test_set=None):

  classifiers = [header_classifier]

  clsf_rst = {}
  regapps = set()
  # rule = Ruler()
  wrong = 0
  sqldao = SqlDao()

  if not test_set:
    test_set = load_pkgs()

  rst = {}
  for package in test_set:
    app = package.app
    company = package.company
    id = package.id

    for classifier in classifiers:
      identifier = classifier(package)
      if identifier != None and identifier != '':
        regapps.add(app)
        rst[id] = app
        if writedb:
          clsf_rst[id] = identifier[1]
        if identifier[0] != package.app:
          wrong += 1

    # if id not in clsf_rst:
    #   clsrst = rule.classify(package)
    #   if clsrst: 
    #     clsf_rst[id] = clsrst[1]
    #     rst[id] = clsrst[0]

    #     companies = set(clsrst[0].split('$'))
    #     if not company:
    #       company = ''

    #     if clsrst[0] != app.lower() and company.lower() not in companies:
    #       wrong += 1


  print 'Finish'
  print "Number of recognized packages:", len(rst), "Wrong:", wrong, 'Total:', len(test_set)
  if writedb:
    upquery = "update packages set classified = %s where id = %s"
    for k, v in clsf_rst.items():
      sqldao.execute(upquery, (v, k))
  sqldao.close()
  return rst

if __name__ == '__main__':
  classify(True)



  #########################################
  # def x_requets_classifer(package):
  # 	if 'X-Requested-With' in package.add_header:		
  # 		return (package.add_header.replace('X-Requested-With',''),1)
  # 	return None

  # def x_unity_version(package):
  # 	if 'X-Unity-Version' in package.add_header:		
  # 		return (package.add_header.replace('X-Unity-Version',''),1)
  # 	return None

  # def X_Umeng_Sdk(package):
  # 	if 'X-Umeng-Sdk' in package.add_header:		
  # 		return (package.add_header.replace('X-Umeng-Sdk',''),1)
  # 	return None

  # def bundle_id(package):
  # 	"""
  # 	services.bearhugmedia.com
  # 	"""
  # 	if u'bundle_id' in package.querys:		
  # 		return (package.querys['bundle_id'][0],2)
  # 	return None

  # def app_id(package):
  # 	"""
  # 	mopub
  # 	"""
  # 	if u'app_id' in package.querys:		
  # 		return (package.querys['app_id'][0],2)
  # 	return None

  # def app_name(package):
  # 	"""
  # 	mopub
  # 	googleads.g.doubleclick.net
  # 	"""
  # 	if u'app_name' in package.querys:		
  # 		return (package.querys['app_name'][0],2)
  # 	return None

  # def appid(package):
  # 	"""
  # 	s28449.ads.madgic.com
  # 	aax-us-east.amazon-adsystem.com
  # 	"""
  # 	if u'appid' in package.querys:		
  # 		return (package.querys['appid'][0],2)
  # 	return None

  # def source_appstore_id(package):
  # 	if u'source_app_store_id' in package.querys:
  # 		return (package.querys['source_app_store_id'][0],2)
  # 	return None

  # def bnd(package):
  # 	"""
  # 	t.manage.com
  # 	"""
  # 	if u'bnd' in package.querys:
  # 		return (package.querys['bnd'][0],2)
  # 	return None

  # def pkid(package):
  # 	"""
  # 	androidsdk.ads.mp.mydas.mobi
  # 	"""
  # 	if u'pkid' in package.querys:
  # 		return (package.querys['pkid'][0],2)
  # 	return None

  # def packageq(package):
  # 	"""
  # 	ads.mobilecore.com
  # 	"""
  # 	if u'package' in package.querys:
  # 		return (package.querys['package'][0],2)
  # 	return None

  # def appq(package):
  # 	"""
  # 	api2.playhaven.com
  # 	"""
  # 	if u'app' in package.querys:
  # 		return (package.querys['app'][0],2)
  # 	return None

  # def packageId(package):
  # 	"""
  # 	www.startappexchange.com
  # 	"""
  # 	if u'packageId' in package.querys:
  # 		return (package.querys['packageId'][0],2)
  # 	return None

  # def kvappn(package):
  # 	"""
  # 	a.adtechus.com
  # 	"""
  # 	if u'kvappn' in package.querys:
  # 		return (package.querys['kvappn'][0],2)
  # 	return None

  # def refer_appname(package):
  # 	"""
  # 	refer
  # 	"""
  # 	import urlparse
  # 	querys = urlparse.parse_qs(urlparse.urlparse(package.refer).query, True)
  # 	if u'app_name' in querys:
  # 		return (querys['app_name'][0],3)
  # 	return None

  # def refer_app(package):
  # 	if u'app:' in package.refer:
  # 		return (package.refer.replace('app:',''),4)
  # 	return None
