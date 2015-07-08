from utils import load_appinfo, longest_common_substring, get_top_domain
from sqldao import SqlDao
from collections import defaultdict
import consts


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
      urlCompany = defaultdict(set)
      substrCompany = defaultdict(set)
      appCompany, appName = load_appinfo()
      # for app, url, fileName in sqldao.execute('SELECT * FROM url_apk'):
      #     app = app.lower()
      #     url = url.replace('http://', '').replace('www.','').replace('-', '.').split('/')[0].split(':')[0]
      #     topDomain = get_top_domain(url)
      #     if url == None or topDomain == None:
      #       continue
      #     fileApp[fileName].add(appCompany[app])
      #     fileUrl[fileName].add(url)
      #     urlApp[url].add(app)
      #     urlCompany[url].add(appCompany[app])
      #     topDomain = get_top_domain(url)
      #     urlApp[topDomain].add(app)
      #     common_str_pkg = longest_common_substring(url.lower(), app)
      #     substrCompany[common_str_pkg].add(appCompany[app])
      #     common_str_company = longest_common_substring(url.lower(), appCompany[app].lower())
      #     substrCompany[common_str_company].add(appCompany[app])
      #     common_str_name = longest_common_substring(url.lower(), appName[app].lower())
      #     substrCompany[common_str_name].add(appCompany[app])

      for pkgs in records.values():
        for pkg in pkgs:
          app = pkg.app
          if app not in appCompany:
              continue
          url = pkg.host.replace('http://', '').replace('www.','').replace('-', '.').split('/')[0].split(':')[0]
          topDomain = get_top_domain(url)
          if url == None or topDomain == None:
            continue
          
          urlApp[topDomain].add(app)
          urlApp[url].add(app)
          urlCompany[url].add(appCompany[app])
          urlCompany[topDomain].add(appCompany[app])

          common_str = longest_common_substring(url.lower(), app)
          substrCompany[common_str].add(appCompany[app])
          common_str_company = longest_common_substring(url.lower(), appCompany[app].lower())
          substrCompany[common_str_company].add(appCompany[app])
          common_str_name = longest_common_substring(url.lower(), appName[app].lower())
          substrCompany[common_str_name].add(appCompany[app])
          if topDomain == '28tracks.com':
            print '#TOPDOMAIN'
          if url == 'citynews.rogersdigitalmedia.com.edgesuite.net':
            print 'citynews.rogersdigitalmedia.com.edgesuite.net'
      rmdUrls = set()

      for fileName,urls in fileApp.iteritems():
          if len(urls) > 1:
              for url in fileUrl[fileName]:
                  rmdUrls.add(url)
      ########################
      # Generate Rules
      ########################
      
      covered = set()
      # fw = open('tmp', 'w')
      def checkCommonStr(app, url):
        for astr in [app, appCompany[app], appName[app]]:
          common_str = longest_common_substring(url.lower(), astr.lower())
          if url == 'citynews.ca':
              print common_str
              print substrCompany[common_str]
          if len(substrCompany[common_str]) < 5 and app in expApp:
              covered.add(app)
              # try:
              #   fw.write( "%s %s %s %s %s %s\n" %(url, app, common_str, appCompany[app], appName[app] , substrCompany[common_str]))
              # except:
              #   pass
              #self.rules[url] = app
              if url == 'citynews.ca':
                  print 'INNNNNNNNNNNN'

              return True
        return False


      
      self.rules = defaultdict(dict)
      for url, apps in urlApp.iteritems():
        companySet = {appCompany[app] for app in apps}
        if url == '1citynews.ca':
          print '#', url in rmdUrls
          print '#', len(apps)
          print apps
          print companySet

        if url not in rmdUrls and (len(apps) == 1 or len(companySet) == 1):
          ruleType = consts.APP_RULE if len(apps) == 1 else consts.COMPANY_RULE

          ifValidRule = True

          for app in apps:
            if checkCommonStr(app, url): 
              ifValidRule = True

          app = apps.pop()
          
          label = app if ruleType == consts.APP_RULE else companySet.pop()

          if ifValidRule:
            self.rules[ruleType][url] = label

          if url == 'citynews.ca':
            print 'Rule Type is', ruleType, ifValidRule

      # fw.close()
      return self

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
          print app, pkg.app, host, secdomain, hostRule, secdomainRule
        rst = {consts.APP_RULE : [(app, 1.0)]}
      elif company:
        rst = {consts.COMPANY_RULE : [(company, 1.0)]}
      return rst


if __name__ == '__main__':
  records = load_pkgs()
  miner = HostApp()
  for record in records:
    miner.process(record)
  r1, r2 = miner.result()
  for k,v in r1.iteritems():
    print k, v
