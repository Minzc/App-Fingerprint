import sys
import operator
from sqldao import SqlDao
from fp_growth import find_frequent_itemsets
from utils import loadfile, rever_map, agent_clean
from itertools import imap
from collections import defaultdict, namedtuple
import const.consts as consts

DEBUG_INDEX = None
DEBUG_ITEM = 'Mrd/1.2.1 (Linux; U; Android 5.0.2; google Nexus 7) com.crossfield.casinogame_bingo/20'.lower()

Rule = namedtuple('Rule', 'rule, label, host, confidence, support')

def _get_package_f(package):
    """Get package features"""
    features = filter(None, map(lambda x: x.strip(), package.path.split('/')))
    if package.json : features += package.json
    # features.append(package.agent)
    host = package.host if package.host else package.dst
    features.append(host)

    return features


test_str = 'com.fdgentertainment.bananakong/1.8.1 (Linux; U; Android 5.0.2; en_US; razor) Apache-HttpClient/UNAVAILABLE (java 1.4)'.lower()
def _encode_data(packages=None, minimum_support = 2):
    def _get_transactions(packages):
      """Change package to transaction"""
      f_counter = defaultdict(int)
      f_company = defaultdict(set)
      processed_transactions = []

      for package in packages:
          transaction = _get_package_f(package)
          transaction.append(package.label)
          transaction.append(package.tbl)
          processed_transactions.append(transaction)
          # Last item is table name
          # Second from last is app
          # Third from last is host. 
          # Do not use them as feature
          for item in transaction[:-3]:
            f_counter[item] += 1
            f_company[item].add(package.company)
      # Get frequent 1-item
      items = {item for item, num in f_counter.iteritems() 
          if num > minimum_support and len(f_company[item]) < 4}
      return processed_transactions, items
    

    processed_transactions, items = _get_transactions(packages)


    itemIndx = defaultdict(lambda: len(itemIndx))
    packageNInfo = {}
    def _encode_transaction(transaction):
        """Change string items to numbers"""
        host = transaction[-3]
        label = transaction[-2]
        tbl = transaction[-1]
        # Prune infrequent items
        # Host and app are not included in transaction now
        encode_transaction = [ itemIndx[item] for item in set(transaction[:-3])
                if item in items]
        packageNInfo[frozenset(encode_transaction)] = {'Tbl': tbl, 'Host': host}

        return (label, encode_transaction)

    train_data = []
    for trans_tuple in imap(_encode_transaction, processed_transactions):
        train_data.append(trans_tuple)

    # train_data
    # all features are encoded; decode dictionary is itemIndx
    # ((label, [f1, f2, f3]))
    # 1 is added to avoid index overlap 
    start_indx = len(itemIndx) + 1
    appIndx = defaultdict(lambda : start_indx + len(appIndx))
    encodedpackages = []


    for app, encode_transaction in train_data:
        # host = encode_transaction[-1]
        # Append class lable at the end of a transaction
        packageNInfo[frozenset(encode_transaction)]['Label'] = appIndx[app]
        encode_transaction.append(appIndx[app])
        encodedpackages.append(encode_transaction)

    print 'Item index is ', itemIndx[test_str]
    # encodedpackages: ([Features, app])
    return encodedpackages, rever_map(appIndx), rever_map(itemIndx), packageNInfo

def _gen_rules(transactions, tSupport, tConfidence, featureIndx, appIndx):
    '''
    Generate encoded rules
    Input
    - transactions : encoded transaction
    - tSupport     : frequent pattern support
    - tConfidence  : frequent pattern confidence
    - featureIndx  : a map between number and feature string
    - appIndex     : a map between number and app
    Return : (itemsets, confidencet, support, label)
    '''
    ###########################
    # FP-tree Version
    ###########################
    print '_gen_rules', featureIndx.keys()[:10]
    rules = set()
    frequentPatterns = find_frequent_itemsets(transactions, tSupport, True)
    for frequent_pattern_info in frequentPatterns:
      itemset, support, tag_dist = frequent_pattern_info
      ruleStrSet = frozenset({featureIndx[itemcode] for itemcode in itemset})
      labelIndex = max(tag_dist.iteritems(), key=operator.itemgetter(1))[0]
      if tag_dist[labelIndex] * 1.0 / support >= tConfidence:
          confidence = max(tag_dist.values()) * 1.0 / support
          rules.add((ruleStrSet, confidence, support, appIndx[labelIndex]))
    
    print ">>> Finish Rule Generating. Total number of rules is", len(rules)
    return rules

def _remove_duplicate(rawRules):
  '''
  Input
  - rawRules : [(ruleStrSet, confidence, support, label), ...]
  '''
  rules = defaultdict(list)
  for rule in rawRules:
    rules[rule[3]].append(rule)
  prunedRules = []
  print 'Total number of rules', len(rawRules)
  for label in rules:
    '''From large to small set'''
    sortedRules = sorted(rules[label], key = lambda x: len(x[0]), reverse = True)
    root = {}
    for i in range(len(sortedRules)):
      print i, len(sortedRules)
      ifKeep = True
      iStrSet = sortedRules[0]
      for j in range(i + 1, len(sortedRules)):
        jStrSet = sortedRules[j][0]
        if jStrSet.issubset(iStrSet):
          ifKeep = False
      if ifKeep:
        prunedRules.append(sortedRules[i])
  return prunedRules


def _prune_rules(tRules, trainData, min_cover = 3):
  '''
  Input t_rules: ( rules, confidence, support, class_label ), get from _gen_rules
  Input packages: list of packets
  Return
  - specificRules host -> ruleStrSet -> label -> {consts.SUPPORT, consts.SCORE}
  defaultdict(lambda : defaultdict(lambda : defaultdict(lambda : { consts.SCORE: 0, consts.SUPPORT: 0 })))

  '''
  import datetime
  ts = datetime.datetime.now()
  cover_num = defaultdict(int)
  index_packages = defaultdict(list)
  tblSupport = defaultdict(set)
  for tbl, packages in trainData.iteritems():
    packageInfos = defaultdict(set)
    for package in packages:
      featuresNhost = _get_package_f(package)
      features = frozenset(featuresNhost[:-1])
      host = featuresNhost[-1]
      packageInfos[package.label].add((features, host))
    print 'Len of Rules is', len(tRules)
    for rule, confidence, support, classlabel in tRules:
      for packageInfo in packageInfos[classlabel]:
        features, host = packageInfo
        if rule.issubset(features):
            tblSupport[rule].add(tbl)

  tRules = sorted(tRules, key = lambda x : len(tblSupport[x]), reverse = True)
  specificRules = defaultdict(lambda : defaultdict(lambda : defaultdict(lambda : { consts.SCORE: 0, consts.SUPPORT: 0 })))
  for rule in tRules:
    ruleStrSet, confidence, support, classlabel = rule
    for packageInfo in packageInfos[classlabel]:
      package, info = packageInfo
      host = info[1]
      if cover_num[package] <= min_cover and ruleStrSet.issubset(package):
        cover_num[package] += 1
        specificRules[host][ruleStrSet][classlabel][consts.SCORE] = confidence
        specificRules[host][ruleStrSet][classlabel][consts.SUPPORT] = len(tblSupport[rule])
  print ">>> Pruning time:", (datetime.datetime.now() - ts).seconds
  return specificRules



class CMAR:
  def __init__(self, min_cover=3, tSupport = 2, tConfidence = 0.8):
    # feature, app, host
    self.rules = defaultdict(lambda : defaultdict(lambda : defaultdict()))
    self.min_cover = min_cover
    self.tSupport = tSupport
    self.tConfidence = tConfidence

  def _persist(self, rules):
    '''specificRules[rule.host][ruleStrSet][label][consts.SCORE] = rule.support''' 
    sqldao = SqlDao()
    QUERY = consts.SQL_INSERT_CMAR_RULES
    params = []
    for ruleType in rules:
      for host in rules[ruleType]:
        for ruleStrSet in rules[ruleType][host]:
          for label, scores in rules[ruleType][host][ruleStrSet].items():
            confidence, support = scores[consts.SCORE], scores[consts.SUPPORT]
            print (label, ','.join(ruleStrSet), confidence, support, host, ruleType)
            params.append((label, ','.join(ruleStrSet), confidence, support, host, ruleType))
      sqldao.executeBatch(QUERY, params)
      sqldao.close()
      print "Total Number of Rules is", len(rules)

  def train(self, trainData, rule_type):
    packages = []
    for tblPackages in trainData.values():
      packages += tblPackages
    print "#CMAR:", len(packages)
    encodedpackages, appIndx, featureIndx, packageNInfo = _encode_data(packages)
    ''' Rules format : (feature, confidence, support, label) '''
    rules = _gen_rules(encodedpackages, self.tSupport, self.tConfidence, featureIndx, appIndx)
    ''' Prune duplicated rules'''
    #rules = _remove_duplicate(rules)
    ''' feature, app, host '''
    specificRules = _prune_rules(rules, trainData, self.min_cover)
    ''' change encoded features back to string '''

    self.rules[rule_type] = specificRules
    self._persist(self.rules)
    self.__init__()
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
    

  def _clean_db(self, rule_type):
    sqldao = SqlDao()
    sqldao.execute( consts.SQL_DELETE_CMAR_RULES % (rule_type))
    sqldao.commit()
    sqldao.close()

  def _count(self, specificRules, trainData):
    '''
    Input
    - rules
        specificRules[rule.host][ruleStrSet][label][consts.SCORE] = rule.support
    Return {type:[(label, confidence)]}
    '''
    labelRsts = {}
    for tbl, packages in trainData.iteritems():
      for package in packages:
        if package.host not in self.specificRules:
          continue
        rules = self.specificRules[package.host]
        features = _get_package_f(package)[:-1]
        for rule in rules:
          if rule.issubset(features):
            rule[package.app][consts.SUPPORT].add(tbl)
    return specificRules

  def classify(self, package):
    '''
    Return {type:[(label, confidence)]}
    '''
    labelRsts = {}
    features = _get_package_f(package)[:-1]
    for rule_type, rules in self.rules.iteritems():
      rst = consts.NULLPrediction
      max_confidence = 0
      if package.host in rules.keys():
          for rule, label_confidence in rules[package.host].iteritems():
              label, confidence = label_confidence
              if rule.issubset(features): #and confidence > max_confidence:
                max_confidence = confidence
                rst = consts.Prediction(label, confidence, rule)

      labelRsts[rule_type] = rst
      if rule_type == consts.APP_RULE and rst != consts.NULLPrediction and rst.label != pkg.app:
        print rst, package.app
        print '=' * 10
    return labelRsts
