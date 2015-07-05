from utils import load_appinfo, longest_common_substring, get_top_domain
from sqldao import SqlDao
from collections import defaultdict


class HostApp:
    def loadExpApp(self):
      expApp=set()
      for app in open("resource/exp_app.txt"):
          expApp.add(app.strip().lower())
      return expApp
        
    
    def train(self, records):
      expApp = self.loadExpApp()
      sqldao = SqlDao()
      fileApp = defaultdict(set)
      fileUrl = defaultdict(set)
      urlApp = defaultdict(set)
      substrCompany = defaultdict(set)
      appCompany, appName = load_appinfo()
      for app, url, fileName in sqldao.execute('SELECT * FROM url_apk'):
          app = app.lower()
          url = url.replace('http://', '').replace('www.','').replace('-', '.').split('/')[0].split(':')[0]
          topDomain = get_top_domain(url)
          fileApp[fileName].add(appCompany[app])
          fileUrl[fileName].add(url)
          urlApp[url].add(app)
          topDomain = get_top_domain(url)
          urlApp[topDomain].add(app)
          common_str_pkg = longest_common_substring(url.lower(), app)
          substrCompany[common_str_pkg].add(appCompany[app])
          common_str_company = longest_common_substring(url.lower(), appCompany[app].lower())
          substrCompany[common_str_company].add(appCompany[app])
          common_str_name = longest_common_substring(url.lower(), appName[app].lower())
          substrCompany[common_str_name].add(appCompany[app])

      for pkgs in records.values():
        for pkg in pkgs:
          app = pkg.app
          if app not in appCompany:
              continue
          url = pkg.host.replace('http://', '').replace('www.','').replace('-', '.').split('/')[0].split(':')[0]
          topDomain = get_top_domain(url)
          urlApp[topDomain].add(app)
          urlApp[url].add(app)
          common_str = longest_common_substring(url.lower(), app)
          substrCompany[common_str].add(appCompany[app])
          common_str_company = longest_common_substring(url.lower(), appCompany[app].lower())
          substrCompany[common_str_company].add(appCompany[app])
          common_str_name = longest_common_substring(url.lower(), appName[app].lower())
          substrCompany[common_str_name].add(appCompany[app])
          if topDomain == 'maps.moovitapp.com':
              print '#TOPDOMAIN'
      rmdUrls = set()

      for fileName,urls in fileApp.iteritems():
          if len(urls) > 1:
              for url in fileUrl[fileName]:
                  rmdUrls.add(url)
      ########################
      # Generate Rules
      ########################
      covered = set()
      self.rules = {}
      for url, apps in urlApp.iteritems():
          if url == 'flixster.com':
              print '#', url in rmdUrls
              print '#', len(apps)
              print apps
          if url not in rmdUrls and len(apps) == 1:
              app = apps.pop()
              for astr in [app, appCompany[app], appName[app]]:
                  common_str = longest_common_substring(url.lower(), astr.lower())
                  if url == 'flixster.com':
                      print common_str
                      print substrCompany[common_str]
                  if len(substrCompany[common_str]) < 5 and app in expApp:
                      covered.add(app)
                      self.rules[url] = app
                      if url == 'flixster.com':
                          print 'INNNNNNNNNNNN'
      return self

    def classify(self, pkg):
      print 'start'
      host = pkg.host.replace('-','.')
      secdomain = pkg.secdomain.replace('-', '.')
      app = self.rules[host] if host in self.rules else None
      app = self.rules[secdomain] if (app == None and secdomain in self.rules) else app
      print app
      return {pkg.id : [(app, 1.0)]}


if __name__ == '__main__':
  records = load_pkgs()
  miner = HostApp()
  for record in records:
    miner.process(record)
  r1, r2 = miner.result()
  for k,v in r1.iteritems():
    print k, v
