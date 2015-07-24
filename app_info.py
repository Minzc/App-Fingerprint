import consts
from sqldao import SqlDao
class App:
  def __init__(self, package, name, company, trackId, website, app_type):
    self.package = package
    self.name = name if len(name) > 0 else 'UNK'
    self.company = company if len(company) > 0 else 'UNK'
    self.trackId
    self.website = website if len(website) > 0 else 'UNK'
    self.app_type = app_type 
class AppInfos:
  def __init__(self):
    sqldao = SqlDao()
    QUERY = 'SELECT package_name, app_title, offered_by, category_code, website FROM android_app_details'
    self.apps = {}
    for package_name, app_title, offered_by, category_code, website in sqldao.execute(QUERY):
      appInfo = App(package_name, app_title, offered_by, 'UNK', website, consts.ANDROID)
      self.apps[package_name] = appInfo
    QUERY = 'SELECT trackId, bundleId, trackName,artistName, primaryGenreName, sellerUrl FROM ios_app_details'
    for trackId, bundleId, trackName, artistName, primaryGenreName, sellerUrl in sqldao.execute(QUERY):
        trackId = str(trackId)
        appInfo = App(bundleId, trackName, artistName, trackId, sellerUrl, consts.IOS)
        self.apps[trackId] = appInfo
    sqldao.close()

  def get_app(self, query):
    return self.apps.get(query, None)
