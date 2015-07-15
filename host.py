from utils import load_appinfo, longest_common_substring, get_top_domain
from sqldao import SqlDao
from collections import defaultdict
import consts


class HostApp:
    def __init__(self):
      self.fileApp = defaultdict(set)
      self.fileUrl = defaultdict(set)
      self.urlApp = defaultdict(set)
      self.urlCompany = defaultdict(set)
      self.substrCompany = defaultdict(set)
      self.appCompany, self.appName = load_appinfo()

    def loadExpApp(self):
      expApp=set()
      for app in open("resource/exp_app.txt"):
          expApp.add(app.strip().lower())
      return expApp
    
    def persist(self, patterns):
      self._clean_db()
      sqldao = SqlDao()
      QUERY = 'INSERT INTO patterns (label, support, confidence, host, rule_type) VALUES (%s, %s, %s, %s, %s)'
      params = []
      for ruleType in patterns:
        for url, label in patterns[ruleType].iteritems():
          params.append((label, 1, 1, url, ruleType))
      sqldao.executeBatch(QUERY, params)

    def count(self, pkg):
      app = pkg.app
      if app not in self.appCompany:
        return
      url = pkg.host.replace('http://', '').replace('www.','').replace('-', '.').split('/')[0].split(':')[0]
      topDomain = get_top_domain(url)
      if url == None or topDomain == None:
        return
      
      self.urlApp[topDomain].add(app)
      self.urlApp[url].add(app)
      self.urlCompany[url].add(self.appCompany[app])
      self.urlCompany[topDomain].add(self.appCompany[app])
      def addCommonStr(url, app, string):
        common_str = longest_common_substring(url.lower(), string.lower())
        self.substrCompany[common_str].add(self.appCompany[app])

      addCommonStr(url, app, app)
      addCommonStr(url, app, self.appCompany[app].lower())
      addCommonStr(url, app, self.appName[app].lower())
      
      if topDomain == '1nflximg.net':
        print '#TOPDOMAIN'
      if url == '1citynews.rogersdigitalmedia.com.edgesuite.net':
        print 'citynews.rogersdigitalmedia.com.edgesuite.net'
    
    def checkCommonStr(self, app, url, expApp):
      for astr in [app, self.appCompany[app], self.appName[app]]:
        common_str = longest_common_substring(url.lower(), astr.lower())
        if url == '1nflximg.net':
          print common_str
          print self.substrCompany[common_str]
        if len(self.substrCompany[common_str]) < 5 and app in expApp:
          if url == '1nflximg.net':
            print 'INNNNNNNNNNNN'
          return True
      return False

    def _clean_db(self):
      QUERY = "DELETE FROM patterns WHERE paramkey IS NULL and pattens IS NULL"
      sqldao = SqlDao()
      sqldao.execute(QUERY)
      sqldao.close()

    def train(self, records):
      
      expApp = self.loadExpApp()
      #self.usePcapUrl()
      for pkgs in records.values():
        for pkg in pkgs:
          self.count(pkg)
      rmdUrls = set()

      for fileName,urls in self.fileApp.iteritems():
        if len(urls) > 1:
          for url in self.fileUrl[fileName]:
            rmdUrls.add(url)
      print 'rmdUrls', len(rmdUrls)
      ########################
      # Generate Rules
      ########################
      
      self.rules = defaultdict(dict)
      for url, apps in self.urlApp.iteritems():
        companySet = {self.appCompany[app] for app in apps}
        if url == '1nflximg.net':
          print '#', url in rmdUrls
          print '#', len(apps)
          print apps
          print companySet

        if url not in rmdUrls and (len(apps) == 1 or len(companySet) == 1):
          ruleType = consts.APP_RULE if len(apps) == 1 else consts.COMPANY_RULE

          ifValidRule = False

          for app in apps:
            if self.checkCommonStr(app, url, expApp): 
              ifValidRule = True

          app = apps.pop()
          
          label = app if ruleType == consts.APP_RULE else companySet.pop()

          if ifValidRule:
            self.rules[ruleType][url] = label

          if url == '1nflximg.net':
            print 'Rule Type is', ruleType, ifValidRule

      self.persist(self.rules)
      self._cleanup()
      # fw.close()
      return self

    def _cleanup(self):
      self.fileApp = None
      self.fileUrl = None
      self.urlApp = None
      self.urlCompany = None
      self.substrCompany = None
      self.appCompany = None
      self.appName = None

    def loadRules(self):
      self.rules = defaultdict(dict)
      QUERY = "SELECT host, label, rule_type FROM patterns WHERE paramkey is NULL and pattens is NULL"
      sqldao = SqlDao()
      counter = 0
      for host, label, ruleType in sqldao.execute(QUERY):
        counter += 1
        self.rules[ruleType][host] = label
      print '>>> [Host Rules#loadRules] total number of rules is', counter
      sqldao.close()

    
    def classify(self, pkg):
      import consts
      host = pkg.host.replace('-','.')
      secdomain = pkg.secdomain.replace('-', '.')
      appRules = self.rules[consts.APP_RULE]
      companyRules = self.rules[consts.COMPANY_RULE]
      app = None
      app = appRules[host] if host in appRules else None
      app = appRules[secdomain] if (app == None and secdomain in appRules) else app
      company = companyRules[host] if host in companyRules else None
      company = companyRules[secdomain] if (company == None and secdomain in companyRules) else company
      rst = {}
      if app:
        if app != pkg.app:
          hostRule = ''
          if host in appRules:
            hostRule = appRules[host]
          secdomainRule = ''
          if secdomain in appRules:
            secdomainRule = appRules[secdomain]
          # print app, pkg.app, host, secdomain, hostRule, secdomainRule
        rst = {consts.APP_RULE : [(app, 1.0)]}
      elif company:
        rst = {consts.COMPANY_RULE : [(company, 1.0)]}
      return rst
    
    def usePcapUrl(self):
        sqldao = SqlDao()
        for app, url, fileName in sqldao.execute('SELECT * FROM url_apk'):
            app = app.lower()
            url = url.replace('http://', '').replace('www.','').replace('-', '.').split('/')[0].split(':')[0]
            topDomain = get_top_domain(url)
            if url == None or topDomain == None:
              continue
            self.fileApp[fileName].add(self.appCompany[app])
            self.fileUrl[fileName].add(s)
            self.urlApp[url].add(app)
            self.urlCompany[url].add(self.appCompany[app])
            topDomain = get_top_domain(url)
            self.urlApp[topDomain].add(app)
            common_str_pkg = longest_common_substring(url.lower(), app)
            self.substrCompany[common_str_pkg].add(self.appCompany[app])
            common_str_company = longest_common_substring(url.lower(), self.appCompany[app].lower())
            self.substrCompany[common_str_company].add(self.appCompany[app])
            common_str_name = longest_common_substring(url.lower(), self.appName[app].lower())
            self.substrCompany[common_str_name].add(self.appCompany[app])
class PathApp:

  def __init__(self):
    self.fileApp = defaultdict(set)
    self.fileUrl = defaultdict(set)
    self.pathApp = defaultdict(set)
    self.pathCompany = defaultdict(set)
    self.substrCompany = defaultdict(set)
    self.appCompany, self.appName = load_appinfo()

  def loadExpApp(self):
    expApp=set()
    for app in open("resource/exp_app.txt"):
        expApp.add(app.strip().lower())
    return expApp
  
  def count(self, pkg) :
    app = pkg.app
    if app not in self.appCompany:
      return
    path = pkg.path
    if path == None:
      return
    for pathSeg in path.split('/'):
      self.pathApp[pathSeg].add(app)
      self.pathCompany[pathSeg].add(self.appCompany[app])

      common_str = longest_common_substring(pathSeg.lower(), app)
      self.substrCompany[common_str].add(self.appCompany[app])
      common_str_company = longest_common_substring(pathSeg.lower(), self.appCompany[app].lower())
      self.substrCompany[common_str_company].add(self.appCompany[app])
      common_str_name = longest_common_substring(pathSeg.lower(), self.appName[app].lower())
      self.substrCompany[common_str_name].add(self.appCompany[app])
      if pathSeg == '28tracks.com':
        print '#TOPDOMAIN'
      if pathSeg == 'citynews.rogersdigitalmedia.com.edgesuite.net':
        print 'citynews.rogersdigitalmedia.com.edgesuite.net'

  def checkCommonStr(self, app, pathSeg, expApp):
    for astr in [app, self.appCompany[app], self.appName[app]]:
      common_str = longest_common_substring(pathSeg.lower(), astr.lower())
      if pathSeg == '12gigs.helpshift.com':
          print common_str
          print self.substrCompany[common_str]
      if len(self.substrCompany[common_str]) < 5 and app in expApp:
          if pathSeg == '12gigs.helpshift.com':
              print 'INNNNNNNNNNNN'

          return True
    return False
  def train(self, training_data):
    expApp = self.loadExpApp()
    for pkgs in training_data.values():
      for pkg in pkgs:
        self.count(pkg)
    rmdUrls = set()

    for fileName,urls in self.fileApp.iteritems():
        if len(urls) > 1:
            for url in self.fileUrl[fileName]:
                rmdUrls.add(url)
    ########################
    # Generate Rules
    ########################
    
    # fw = open('tmp', 'w')
    


    
    self.rules = defaultdict(dict)
    for pathSeg, apps in self.pathApp.iteritems():
      companySet = {self.appCompany[app] for app in apps}
      if pathSeg == '1citynews.ca':
        print '#', pathSeg in rmdUrls
        print '#', len(apps)
        print apps
        print companySet

      if pathSeg not in rmdUrls and (len(apps) == 1 or len(companySet) == 1):
        ruleType = consts.APP_RULE if len(apps) == 1 else consts.COMPANY_RULE

        ifValidRule = False

        for app in apps:
          if self.checkCommonStr(app, pathSeg, expApp): 
            ifValidRule = True

        app = apps.pop()
        
        label = app if ruleType == consts.APP_RULE else companySet.pop()

        if ifValidRule:
          self.rules[ruleType][pathSeg] = label

        if pathSeg == '12gigs.helpshift.com':
          print 'Rule Type is', ruleType, ifValidRule

    # fw.close()
    return self

  def classify(self, pkg):
    import consts
    path = pkg.path
    appRules = self.rules[consts.APP_RULE]
    companyRules = self.rules[consts.COMPANY_RULE]
    app = None
    for pathSeg in path.split('/'):
      app = appRules[pathSeg] if pathSeg in appRules else None
      company = companyRules[pathSeg] if pathSeg in companyRules else None
      rst = defaultdict(list)
      if app:
        if app != pkg.app:
          hostRule = ''
          if pathSeg in appRules:
            hostRule = appRules[pathSeg]
          secdomainRule = ''
          if pathSeg in appRules:
            secdomainRule = appRules[pathSeg]
          # print app, pkg.app, host, secdomain, hostRule, secdomainRule
        rst[consts.APP_RULE].append((app, 1.0))
      elif company:
        rst[consts.COMPANY_RULE].append((company, 1.0))
    return rst

#sqldao = SqlDao()
# for app, url, fileName in sqldao.execute('SELECT * FROM url_apk'):
#     app = app.lower()
#     url = url.replace('http://', '').replace('www.','').replace('-', '.').split('/')[0].split(':')[0]
#     topDomain = get_top_domain(url)
#     if url == None or topDomain == None:
#       continue
#     self.fileApp[fileName].add(self.appCompany[app])
#     self.fileUrl[fileName].add(url)
#     self.urlApp[url].add(app)
#     self.urlCompany[url].add(self.appCompany[app])
#     topDomain = get_top_domain(url)
#     self.urlApp[topDomain].add(app)
#     common_str_pkg = longest_common_substring(url.lower(), app)
#     self.substrCompany[common_str_pkg].add(self.appCompany[app])
#     common_str_company = longest_common_substring(url.lower(), self.appCompany[app].lower())
#     self.substrCompany[common_str_company].add(self.appCompany[app])
#     common_str_name = longest_common_substring(url.lower(), self.appName[app].lower())
#     self.substrCompany[common_str_name].add(self.appCompany[app])
if __name__ == '__main__':
  records = load_pkgs()
  miner = HostApp()
  for record in records:
    miner.process(record)
  r1, r2 = miner.result()
  for k,v in r1.iteritems():
    print k, v
