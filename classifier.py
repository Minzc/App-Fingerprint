from package import Package
from sqldao import SqlDao
from utils import load_pkgs
import consts

DEBUG = False

def header_classifier(package):
    identifier = ['x-umeng-sdk', 'x-vungle-bundle-id', 'x-requested-with']
    for id in identifier:
        for head_seg in package.add_header.split('\n'):
            if id in head_seg and '.' in head_seg:
                return (head_seg.replace(id + ':', '').strip(), 1)
    return None

class HeaderClassifier:
  def __init__(self):
    self.rules = None

  def train(self, train_set):
    return self
    
  def loadRules(self):
    pass
    
  def classify(self,package):
    classifiers = [header_classifier]

    rst = {}
    app, company, id = package.app, package.company, package.id

    for classifier in classifiers:
      identifier = classifier(package)
      if identifier:
        rst[consts.APP_RULE] = [(identifier[0],1.0)]
        if identifier[0] != package.app:
          if DEBUG : print identifier, package.app
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
