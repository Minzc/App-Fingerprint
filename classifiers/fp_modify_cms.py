from utils import longest_common_substring, get_top_domain, url_clean, load_exp_app, load_xml_features
from sqldao import SqlDao
from collections import defaultdict
import const.consts as consts
from const.app_info import AppInfos
from classifier import AbsClassifer
import re

test_str = {}

class CMAR(AbsClassifer):
  def __init__(self, min_cover=1, tSupport = 2, tConfidence = 1.0):
    # feature, app, host
    self.rules = defaultdict(lambda : defaultdict(lambda : defaultdict()))
    self.min_cover = min_cover
    self.tSupport = tSupport
    self.tConfidence = tConfidence
    self.pathLabel = defaultdict(set)
    self.pathTable = defaultdict(set)
    self.substrCompany = defaultdict(set)
    self.appType = consts.IOS
    self.pathCmmStr = defaultdict(lambda : defaultdict(set))

  def _get_package_f(self, package):
    """Get package features"""
    features = filter(None, map(lambda x: x.strip(), package.path.split('/')))
    if package.json : features += package.json
    return features
  
  def persist(self, rules):
    '''specificRules[rule.host][ruleStrSet][label] = set''' 
    sqldao = SqlDao()
    QUERY = consts.SQL_INSERT_CMAR_RULES
    params = []
    for ruleType in rules:
      for host in rules[ruleType]:
        for ruleStrSet in rules[ruleType][host]:
          for label, scores in rules[ruleType][host][ruleStrSet].items():
            support = len(scores)
            confidence = 1
            params.append((label, ruleStrSet, confidence, support, host, ruleType))
      sqldao.executeBatch(QUERY, params)
      sqldao.close()
      print "Total Number of Rules is", len(rules)

  def count(self, records):
    def addCommonStr(pathSeg, features):
      for string in features:
        commonStr = longest_common_substring(pathSeg.lower(), string.lower())
        commonStr = commonStr.strip('.')
        if len(commonStr) > 2:
          self.substrCompany[commonStr].add(pkg.company)
          self.pathCmmStr[label][pathSeg].add(commonStr)

    for tbl, pkgs in records.iteritems():
      for pkg in pkgs:
        label = pkg.label
        appInfo = pkg.appInfo
        pathSegs = self._get_package_f(pkg)
        map(lambda pathSeg : self.pathLabel[pathSeg].add(label), pathSegs)
        map(lambda pathSeg : self.pathTable[pathSeg].add(tbl), pathSegs)

    for pathSeg, labels in self.pathLabel.iteritems():
      if len(labels) == 1 and len(self.pathTable[pathSeg]) > 2:
        label = labels.pop()
        addCommonStr(pathSeg, self.fLib[label])
        labels.add(label)
  
  def checkCommonStr(self, label, pathSeg, expApp):
    for astr in self.fLib:
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
  
  def _feature_lib(self, expApp, xmlFeatures):
    self.fLib = defaultdict(set)
    featureCategory = defaultdict(set)
    for appInfo in expApp:
      label = appInfo.package
      appSegs = appInfo.package.split('.')
      companySegs = appInfo.company.split(' ')
      categorySegs = appInfo.category.split(' ')
      wholeSegs = [appSegs, companySegs, categorySegs]
      for segs in wholeSegs:
        for seg in segs:
          self.fLib[label].add(seg)
      self.fLib[label].add(appInfo.package)
      self.fLib[label].add(appInfo.trackId)
      self.fLib[label].add(appInfo.company)
      self.fLib[label].add(appInfo.category)
      self.fLib[label] |= xmlFeatures[label]
      for feature in self.fLib[label]:
        featureCategory[feature].add(appInfo.company)
    '''Only keep strings that are related to one category as features'''
    for label, features in self.fLib.iteritems():
      self.fLib[label] = {f for f in features if len(featureCategory[f]) != 1}


  def train(self, records, rule_type):
    xmlFeatures = load_xml_features()
    expApp = {AppInfos.get(self.appType, label) for label in load_exp_app()[self.appType]}
    self._feature_lib(expApp, xmlFeatures)
    self.count(records)
    ########################
    # Generate Rules
    ########################
    
    
    interestedPathSegs = defaultdict(dict)
    for pathSeg, labels in self.pathLabel.iteritems():
      if pathSeg in test_str:
        print '#', len(labels)
        print labels
        print pathSeg

      if len(labels) == 1:
        label = labels.pop()
        ifValidRule = False
        for commonStr in self.pathCmmStr[label][pathSeg]:
          if len(self.substrCompany[commonStr]) < 5:
            ifValidRule = True

        if ifValidRule:
          interestedPathSegs[rule_type][pathSeg] = label
          print '>>', pathSeg.encode('utf-8'), commonStr.encode('utf-8'), label

        if pathSeg in test_str:
          print 'Rule Type is', rule_type, ifValidRule, pathSeg 

    print 'number of rule', len(self.rules[consts.APP_RULE])

    specificRules = self.count_support(records, interestedPathSegs)
    self.rules[rule_type] = specificRules
    self.persist(self.rules)
    self.__init__(self.appType)
    return self

  def load_rules(self):
    self.rules = {}
    self.rules[consts.APP_RULE] = defaultdict(lambda : defaultdict())
    self.rules[consts.COMPANY_RULE] = defaultdict(lambda : defaultdict())
    self.rules[consts.CATEGORY_RULE] = defaultdict(lambda : defaultdict())
    sqldao = SqlDao()
    counter = 0
    SQL = consts.SQL_SELECT_CMAR_RULES
    for label, patterns, host, ruleType, support in sqldao.execute(SQL):
      counter += 1
      patterns = frozenset(map(lambda x: x.strip(), patterns.split(",")))
      self.rules[ruleType][host][patterns] = (label, support)
    sqldao.close()
    print '>>>[CMAR] Totaly number of rules is', counter
    for ruleType in self.rules:
      print '>>>[CMAR] Rule Type %s Number of Rules %s' % (ruleType, len(self.rules[ruleType]))

  
  def count_support(self, records, interestedPathSegs):
    LABEL = 0
    TBLSUPPORT = 1

    specificRules = defaultdict(lambda : defaultdict(lambda : defaultdict(set)))
    for tbl, pkgs in records.items():
      for pkg in pkgs:
        for ruleType in interestedPathSegs:
          predict = consts.NULLPrediction
          for pathSeg in self._get_package_f(pkg):
            if pathSeg in interestedPathSegs[ruleType]:
              label = interestedPathSegs[ruleType][pathSeg]
              if label == pkg.label:
                specificRules[pkg.host][pathSeg][label].add(tbl)
    return specificRules

  def classify(self, package):
    '''
    Return {type:[(label, confidence)]}
    '''
    labelRsts = {}
    features = self._get_package_f(package)[:-1]
    for rule_type, rules in self.rules.iteritems():
      rst = consts.NULLPrediction
      max_confidence = 0
      if len(package.queries) == 0:
        if package.host in rules.keys():
          for rule, label_confidence in rules[package.host].iteritems():
            label, confidence = label_confidence
            if rule.issubset(features): #and confidence > max_confidence:
              max_confidence = confidence
              rst = consts.Prediction(label, confidence, rule)

      labelRsts[rule_type] = rst
      if rule_type == consts.APP_RULE and rst != consts.NULLPrediction and rst.label != package.app:
        print rst, package.app
        print '=' * 10
    return labelRsts


if __name__ == '__main__':
  records = load_pkgs()
  miner = HostApp()
  for record in records:
    miner.process(record)
  r1, r2 = miner.result()
  for k,v in r1.iteritems():
    print k, v
