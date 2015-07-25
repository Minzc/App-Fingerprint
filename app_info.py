import consts
from sqldao import SqlDao
from collections import defaultdict
from utils import url_clean
class App:
  def __init__(self, package, name, company, trackId, website, category,app_type):
    self.package = package
    self.name = name if name else 'UNK'
    self.company = company if company else 'UNK'
    self.trackId = trackId
    self.website = url_clean(website) if website else 'UNK'
    self.app_type = app_type 
    self.category = category
class AppInfos:
  def __init__(self):
    sqldao = SqlDao()
    QUERY = 'SELECT package_name, app_title, offered_by, category_code, website FROM android_app_details'
    self.apps = defaultdict(dict)
    for package_name, app_title, offered_by, category_code, website in sqldao.execute(QUERY):
      appInfo = App(package_name, app_title, offered_by, 'UNK', website, category_code,consts.ANDROID)
      self.apps[consts.ANDROID][package_name] = appInfo
    QUERY = 'SELECT trackId, bundleId, trackName,artistName, primaryGenreName, sellerUrl FROM ios_app_details'
    for trackId, bundleId, trackName, artistName, primaryGenreName, sellerUrl in sqldao.execute(QUERY):
        trackId = str(trackId)
        appInfo = App(bundleId, trackName, artistName, trackId, sellerUrl, primaryGenreName,consts.IOS)
        self.apps[consts.IOS][trackId] = appInfo
        self.apps[consts.IOS][bundleId] = appInfo
    sqldao.close()

  def get(self, app_type, query):
    return self.apps[app_type].get(query, None)