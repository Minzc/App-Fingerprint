from utils import longest_common_substring, get_top_domain, url_clean, load_exp_app
from sqldao import SqlDao
from collections import defaultdict
import const.consts as consts
from const.app_info import AppInfos
from classifier import AbsClassifer
import re

#test_str = {'1.ticketm.net', 't.homedepot.com', 'pepperjamnetwork.com', 'players.brightcove.net', 'pagefair.com'}
test_str = {'sharesdk.cn'}

class HostApp(AbsClassifer):
    def __init__(self, appType):
      self.appType = appType
      self.urlLabel = defaultdict(set)
      self.substrCompany = defaultdict(set)
      self.labelAppInfo = {}
      self.rules = defaultdict(dict)

    
    def persist(self, patterns, rule_type):
      self._clean_db(rule_type)
      sqldao = SqlDao()
      QUERY = consts.SQL_INSERT_HOST_RULES
      params = []
      for ruleType in patterns:
        for url, labelNsupport in patterns[ruleType].iteritems():
          label, support = labelNsupport
          params.append((label, len(support), 1, url, ruleType))
      sqldao.executeBatch(QUERY, params)
      sqldao.close()

    def count(self, pkg):
      def addCommonStr(url, pkg, string):
        common_str = longest_common_substring(url.lower(), string.lower())
        common_str = common_str.strip('.')
        if len(common_str) > 3 or (common_str in self.fLib[pkg.label] and len(common_str) > 1):
          self.substrCompany[common_str].add(pkg.label)

      label = pkg.label
      appInfo = pkg.appInfo
      url = url_clean(pkg.host)
      top_domain = get_top_domain(url)
      refer_host = pkg.refer_host
      refer_top_domain = get_top_domain(refer_host)
      if not url or not top_domain:
        return

      if not appInfo:
        print '>>>[HOST] ERROR app is', pkg.app
        return
      
      self.labelAppInfo[label] = (pkg.app, pkg.company, pkg.category)
      map(lambda url : self.urlLabel[url].add(label), [top_domain, url, refer_host, refer_top_domain])
      map(lambda string : addCommonStr(url, pkg, string), [pkg.app, pkg.company, pkg.name, pkg.website])
    
    def checkCommonStr(self, label, url, expApp):
      for astr in self.labelAppInfo[label]:
        common_str = longest_common_substring(url.lower(), astr.lower())
        common_str = common_str.strip('.')
        if url in test_str:
          print common_str, url
          print self.substrCompany[common_str], url
        subCompanyLen = len(self.substrCompany[common_str])
        strValid = True if common_str in self.fLib[label] and len(common_str) > 1 else False
        companyValid = True if subCompanyLen < 5 and subCompanyLen > 0 else False
          
        if companyValid and strValid:
          if url in test_str:
            print 'INNNNNNNNNNNN', url, label, common_str
          return True
      return False

    def _clean_db(self, rule_type):
      QUERY = consts.SQL_DELETE_HOST_RULES
      sqldao = SqlDao()
      sqldao.execute(QUERY % (rule_type))
      sqldao.close()
    
    def _feature_lib(self, expApp):
      self.fLib = defaultdict(set)
      for label, appInfo in expApp.iteritems():
        appSegs = appInfo.package.split('.')
        companySegs = appInfo.company.split(' ')
        categorySegs = appInfo.category.split(' ')
        wholeSegs = [appSegs, companySegs, categorySegs]
        for segs in wholeSegs:
          for seg in segs:
            self.fLib[label].add(seg)

    def train(self, records, rule_type):
      expApp = load_exp_app()[self.appType]
      expApp = {label: AppInfos.get(self.appType, label) for label in expApp}
      self._feature_lib(expApp)
      for pkgs in records.values():
        for pkg in pkgs:
          self.count(pkg)
      ########################
      # Generate Rules
      ########################
      

      for url, labels in self.urlLabel.iteritems():
        if url in test_str:
          print '#', len(labels)
          print labels
          print url

        if len(labels) == 1:
          label = labels.pop()
          ifValidRule = True if self.checkCommonStr(label, url, expApp) else False

          if ifValidRule:
            self.rules[rule_type][url] = (label, set())

          if url in test_str:
            print 'Rule Type is', rule_type, ifValidRule, url

      print 'number of rule', len(self.rules[consts.APP_RULE])

      self.count_support(records)
      self.persist(self.rules, rule_type)
      self.__init__(self.appType)
      return self

    def load_rules(self):
      self.rules = {consts.APP_RULE:{}, consts.COMPANY_RULE:{}, consts.CATEGORY_RULE:{}}
      QUERY = consts.SQL_SELECT_HOST_RULES
      sqldao = SqlDao()
      counter = 0
      for host, label, ruleType, support in sqldao.execute(QUERY):
        counter += 1
        regexObj =  re.compile(r'\b' + re.escape(host) + r'\b')
        self.rules[ruleType][host] = (label, support, regexObj)
      print '>>> [Host Rules#loadRules] total number of rules is', counter, 'Type of Rules', len(self.rules)
      sqldao.close()

    def load_rules2(self):
      self.rules = {consts.APP_RULE:{}, consts.COMPANY_RULE:{}, consts.CATEGORY_RULE:{}}
      QUERY = consts.SQL_SELECT_HOST_RULES
      sqldao = SqlDao()
      counter = 0
      for host, label, ruleType, support in sqldao.execute(QUERY):
        counter += 1
        self.rules[ruleType][host] = (label, support)
      print '>>> [Host Rules#loadRules] total number of rules is', counter, 'Type of Rules', len(self.rules)
      sqldao.close()
    
    def count_support(self, records):
      LABEL = 0
      TBLSUPPORT = 1
      for tbl, pkgs in records.items():
        for pkg in pkgs:
          for ruleType in self.rules:
            host = url_clean(pkg.host)
            secdomain = get_top_domain(host)
            refer_host = pkg.refer_host
            refer_top_domain = get_top_domain(refer_host)
            predict = consts.NULLPrediction
            for url in [host, secdomain, refer_host, refer_top_domain]:
              if url in self.rules[ruleType]:
                label = self.rules[ruleType][url][LABEL]
                if label == pkg.label:
                  self.rules[ruleType][url][TBLSUPPORT].add(tbl)

    def classify(self, pkg):
      '''
      Input
      - self.rules : {ruleType: {host : (label, support, regexObj)}}
      - pkg : http packet
      '''
      rst = {}
      for ruleType in self.rules:
        predict = consts.NULLPrediction
        for url in [pkg.host]:
          for regexStr, ruleTuple in self.rules[ruleType].iteritems():
            label, support, regexObj = ruleTuple
            match = regexObj.search(pkg.host)
            if match and predict.score < support:
              predict = consts.Prediction(label, support, (pkg.host, regexStr))
            # if pkg.app == 'com.logos.vyrso' and pkg.host == 'gsp1.apple.com':
            #   print regexStr
	      # print match

        rst[ruleType] = predict
        if predict.label != pkg.app and predict.label != None:
          print predict.evidence, pkg.app, predict.label
      return rst

    def classify2(self, pkg):
      rst = {}
      for ruleType in self.rules:
        host = pkg.host.replace('-','.')
        secdomain = pkg.secdomain.replace('-', '.')
        refer_host = pkg.refer_host
        refer_top_domain = get_top_domain(refer_host)
        predict = consts.NULLPrediction
        label = None
        for url in [host, secdomain, refer_host, refer_top_domain]:
          labelNsupport = self.rules[ruleType].get(url, None)
          if labelNsupport != None:
            label, support = labelNsupport
            predict = consts.Prediction(label, 1, url)
            break

        rst[ruleType] = predict
      return rst


if __name__ == '__main__':
  records = load_pkgs()
  miner = HostApp()
  for record in records:
    miner.process(record)
  r1, r2 = miner.result()
  for k,v in r1.iteritems():
    print k, v
