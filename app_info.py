import consts
from sqldao import SqlDao
from collections import defaultdict
class App:
  def __init__(self, package, name, company, trackId, website, category,app_type):
    self.package = package.lower()
    self.name = name.lower() if name else 'UNK'
    self.company = company.lower() if company else 'UNK'
    self.trackId = trackId
    self.website = self.url_clean(website.lower()) if website else 'UNK'
    self.app_type = app_type 
    self.category = category
  def url_clean(self, url):
    url = url.replace('http://', '').replace('www.','').replace('-', '.').split('/')[0].split(':')[0]
    return url

class AppInformations:
  def __init__(self):
    print 'CREATE APPINFO'
    sqldao = SqlDao()
    QUERY = 'SELECT package_name, app_title, offered_by, category_code, website FROM android_app_details'
    self.apps = defaultdict(dict)
    for package_name, app_title, offered_by, category_code, website in sqldao.execute(QUERY):
      package_name = package_name.lower()
      appInfo = App(package_name, app_title, offered_by, 'UNK', website, category_code,consts.ANDROID)
      self.apps[consts.ANDROID][package_name.lower()] = appInfo
    QUERY = 'SELECT trackId, bundleId, trackName,artistName, primaryGenreName, sellerUrl FROM ios_app_details'
    for trackId, bundleId, trackName, artistName, primaryGenreName, sellerUrl in sqldao.execute(QUERY):
        trackId = str(trackId)
        bundleId = bundleId.lower()
        appInfo = App(bundleId, trackName, artistName, trackId, sellerUrl, primaryGenreName,consts.IOS)
        self.apps[consts.IOS][trackId] = appInfo
        self.apps[consts.IOS][bundleId] = appInfo
    sqldao.close()

  def get(self, app_type, query):
    return self.apps[app_type].get(query, None)

AppInfos = AppInformations()
