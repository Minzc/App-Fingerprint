from utils import load_appinfo, longest_common_substring, get_top_domain, url_clean
from sqldao import SqlDao
from collections import defaultdict
import consts
from app_info import AppInfos
from classifier import AbsClassifer

test_str = 'voyagesarabais.com'

class HostApp(AbsClassifer):
    def __init__(self):
      # self.fileApp = defaultdict(set)
      # self.fileUrl = defaultdict(set)
      self.urlLabel = defaultdict(set)
      self.urlCompany = defaultdict(set)
      self.substrCompany = defaultdict(set)
      self.apps = AppInfos()
      self.labelAppInfo = {}
      self.rules = defaultdict(dict)

    def loadExpApp(self):
      expApp=set()
      for app in open("resource/exp_app.txt"):
          expApp.add(app.strip().lower())
      return expApp
    
    def persist(self, patterns, rule_type):
      self._clean_db(rule_type)
      sqldao = SqlDao()
      QUERY = 'INSERT INTO patterns (label, support, confidence, host, rule_type) VALUES (%s, %s, %s, %s, %s)'
      params = []
      for ruleType in patterns:
        for url, label in patterns[ruleType].iteritems():
          params.append((label, 1, 1, url, ruleType))
      sqldao.executeBatch(QUERY, params)
      sqldao.close()

    def count(self, pkg, app_type=consts.ANDROID):
      def addCommonStr(url, pkg, string):
        common_str = longest_common_substring(url.lower(), string.lower())
        self.substrCompany[common_str].add(pkg.label)

      label = pkg.label
      appInfo = self.apps.get(app_type, pkg.app)
      url = url_clean(pkg.host)
      top_domain = get_top_domain(url)
      if not url or not top_domain:
        return

      if not appInfo:
        print '>>>[HOST] ERROR app type is', app_type, 'app is', pkg.app
        return
      
      self.labelAppInfo[label] = (pkg.app, pkg.company, pkg.category)
      map(lambda url : self.urlLabel[url].add(label), [top_domain, url])
      map(lambda url : self.urlCompany[url].add(pkg.company), [top_domain, url])
      map(lambda string : addCommonStr(url, pkg, string), [pkg.app, pkg.company, pkg.name, pkg.website])
    
    def checkCommonStr(self, label, url, expApp):
      for astr in self.labelAppInfo[label]:
        common_str = longest_common_substring(url.lower(), astr.lower())
        if url == test_str:
          print common_str
          print self.substrCompany[common_str]
        if len(self.substrCompany[common_str]) < 5 and self.labelAppInfo[label][0] in expApp:
          if url == test_str:
            print 'INNNNNNNNNNNN'
          return True
      return False

    def _clean_db(self, rule_type):
      QUERY = "DELETE FROM patterns WHERE paramkey IS NULL and pattens IS NULL and rule_type=%s"
      sqldao = SqlDao()
      sqldao.execute(QUERY % (rule_type))
      sqldao.close()

    def train(self, records, rule_type):
      expApp = self.loadExpApp()
      for pkgs in records.values():
        for pkg in pkgs:
          self.count(pkg)
      ########################
      # Generate Rules
      ########################
      
      print test_str in self.urlLabel

      for url, labels in self.urlLabel.iteritems():
        if url == test_str:
          print '#', len(labels)
          print labels

        if len(labels) == 1:
          label = labels.pop()
          ifValidRule = True if self.checkCommonStr(label, url, expApp) else False

          if ifValidRule:
            self.rules[rule_type][url] = label

          if url == test_str:
            print 'Rule Type is', rule_type, ifValidRule

      self.persist(self.rules, rule_type)
      self.__init__()
      # fw.close()
      return self

    def load_rules(self):
      self.rules = {consts.APP_RULE:{}, consts.COMPANY_RULE:{}, consts.CATEGORY_RULE:{}}
      QUERY = "SELECT host, label, rule_type FROM patterns WHERE paramkey is NULL and pattens is NULL"
      sqldao = SqlDao()
      counter = 0
      for host, label, ruleType in sqldao.execute(QUERY):
        counter += 1
        self.rules[ruleType][host] = label
      print '>>> [Host Rules#loadRules] total number of rules is', counter, 'Type of Rules', len(self.rules)
      sqldao.close()

    def classify(self, pkg):
      rst = {}
      for rule_type in self.rules:
        host = pkg.host.replace('-','.')
        secdomain = pkg.secdomain.replace('-', '.')
        label = self.rules[rule_type].get(host, None)
        label = self.rules[rule_type].get(secdomain, None) if not label else label
        if host == 'sd9.radarnowandroid.com':
          print label
        rst[rule_type] = (label, 1.0)
      return rst

    '''    
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
        rst = {consts.COMPANY_RULE : (company, 1.0)}
      return rst
     '''
    
    def usePcapUrl(self):
        sqldao = SqlDao()
        for app, url, fileName in sqldao.execute('SELECT * FROM url_apk'):
            app = app.lower()
            url = url.replace('http://', '').replace('www.','').replace('-', '.').split('/')[0].split(':')[0]
            top_domain = get_top_domain(url)
            if url == None or top_domain == None:
              continue
            self.fileApp[fileName].add(self.appCompany[app])
            self.fileUrl[fileName].add(s)
            self.urlLabel[url].add(app)
            self.urlCompany[url].add(self.appCompany[app])
            top_domain = get_top_domain(url)
            self.urlLabel[top_domain].add(app)
            common_str_pkg = longest_common_substring(url.lower(), app)
            self.substrCompany[common_str_pkg].add(self.appCompany[app])
            common_str_company = longest_common_substring(url.lower(), self.appCompany[app].lower())
            self.substrCompany[common_str_company].add(self.appCompany[app])
            common_str_name = longest_common_substring(url.lower(), self.appName[app].lower())
            self.substrCompany[common_str_name].add(self.appCompany[app])
# class PathApp:

#   def __init__(self):
#     self.fileApp = defaultdict(set)
#     self.fileUrl = defaultdict(set)
#     self.pathApp = defaultdict(set)
#     self.pathCompany = defaultdict(set)
#     self.substrCompany = defaultdict(set)
#     self.appCompany, self.appName = load_appinfo()

#   def loadExpApp(self):
#     expApp=set()
#     for app in open("resource/exp_app.txt"):
#         expApp.add(app.strip().lower())
#     return expApp
  
#   def count(self, pkg) :
#     app = pkg.app
#     if app not in self.appCompany:
#       return
#     path = pkg.path
#     if path == None:
#       return
#     for pathSeg in path.split('/'):
#       self.pathApp[pathSeg].add(app)
#       self.pathCompany[pathSeg].add(self.appCompany[app])

#       common_str = longest_common_substring(pathSeg.lower(), app)
#       self.substrCompany[common_str].add(self.appCompany[app])
#       common_str_company = longest_common_substring(pathSeg.lower(), self.appCompany[app].lower())
#       self.substrCompany[common_str_company].add(self.appCompany[app])
#       common_str_name = longest_common_substring(pathSeg.lower(), self.appName[app].lower())
#       self.substrCompany[common_str_name].add(self.appCompany[app])
#       if pathSeg == '28tracks.com':
#         print '#TOPDOMAIN'
#       if pathSeg == 'citynews.rogersdigitalmedia.com.edgesuite.net':
#         print 'citynews.rogersdigitalmedia.com.edgesuite.net'

#   def checkCommonStr(self, app, pathSeg, expApp):
#     for astr in [app, self.appCompany[app], self.appName[app]]:
#       common_str = longest_common_substring(pathSeg.lower(), astr.lower())
#       if pathSeg == '12gigs.helpshift.com':
#           print common_str
#           print self.substrCompany[common_str]
#       if len(self.substrCompany[common_str]) < 5 and app in expApp:
#           if pathSeg == '12gigs.helpshift.com':
#               print 'INNNNNNNNNNNN'

#           return True
#     return False
#   def train(self, training_data):
#     expApp = self.loadExpApp()
#     for pkgs in training_data.values():
#       for pkg in pkgs:
#         self.count(pkg)
#     ########################
#     # Generate Rules
#     ########################
    
#     # fw = open('tmp', 'w')
    


    
#     self.rules = defaultdict(dict)
#     for pathSeg, apps in self.pathApp.iteritems():
#       companySet = {self.appCompany[app] for app in apps}
#       if pathSeg == '1citynews.ca':
#         print '#', pathSeg in rmdUrls
#         print '#', len(apps)
#         print apps
#         print companySet

#       if len(apps) == 1 or len(companySet) == 1:
#         ruleType = consts.APP_RULE if len(apps) == 1 else consts.COMPANY_RULE

#         ifValidRule = False

#         for app in apps:
#           if self.checkCommonStr(app, pathSeg, expApp): 
#             ifValidRule = True

#         app = apps.pop()
        
#         label = app if ruleType == consts.APP_RULE else companySet.pop()

#         if ifValidRule:
#           self.rules[ruleType][pathSeg] = label

#         if pathSeg == '12gigs.helpshift.com':
#           print 'Rule Type is', ruleType, ifValidRule

#     # fw.close()
#     return self

#   def classify(self, pkg):
#     import consts
#     path = pkg.path
#     appRules = self.rules[consts.APP_RULE]
#     companyRules = self.rules[consts.COMPANY_RULE]
#     app = None
#     for pathSeg in path.split('/'):
#       app = appRules[pathSeg] if pathSeg in appRules else None
#       company = companyRules[pathSeg] if pathSeg in companyRules else None
#       rst = defaultdict(list)
#       if app:
#         if app != pkg.app:
#           hostRule = ''
#           if pathSeg in appRules:
#             hostRule = appRules[pathSeg]
#           secdomainRule = ''
#           if pathSeg in appRules:
#             secdomainRule = appRules[pathSeg]
#           # print app, pkg.app, host, secdomain, hostRule, secdomainRule
#         rst[consts.APP_RULE].append((app, 1.0))
#       elif company:
#         rst[consts.COMPANY_RULE].append((company, 1.0))
#     return rst

if __name__ == '__main__':
  records = load_pkgs()
  miner = HostApp()
  for record in records:
    miner.process(record)
  r1, r2 = miner.result()
  for k,v in r1.iteritems():
    print k, v
