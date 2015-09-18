import sys
import operator
from sqldao import SqlDao
from fp_growth import find_frequent_itemsets
from utils import loadfile, rever_map, agent_clean
from itertools import imap
from collections import defaultdict, namedtuple
import consts

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
          processed_transactions.append(transaction)
          # Last item is app
          # Second from last is host. Do not use host as feature
          for item in transaction[:-2]:
            f_counter[item] += 1
            f_company[item].add(package.company)
      # Get frequent 1-item
      items = {item for item, num in f_counter.iteritems() 
          if num > minimum_support and len(f_company[item]) < 4}
      return processed_transactions, items
    

    processed_transactions, items = _get_transactions(packages)


    itemIndx = defaultdict(lambda: len(itemIndx))
    packageHost = []
    def _encode_transaction(transaction):
        """Change string items to numbers"""
        host = transaction[-2]
        label = transaction[-1]
        # Prune infrequent items
        # Host and app are not included in transaction now
        encode_transaction = [ itemIndx[item] for item in set(transaction[:-2])
                if item in items]

        packageHost.append(host)
        return (label, encode_transaction)

    train_data = []
    for trans_tuple in imap(_encode_transaction, processed_transactions):
        train_data.append(trans_tuple)

    # train_data
    # all features are encoded; decode dictionary is itemIndx
    # ((label, [f1, f2, f3, host]))
    # 1 is added to avoid index overlap 
    start_indx = len(itemIndx) + 1
    appIndx = defaultdict(lambda : start_indx + len(appIndx))
    encodedpackages = []


    for app, encode_transaction in train_data:
        # host = encode_transaction[-1]
        # Append class lable at the end of a transaction
        encode_transaction.append(appIndx[app])
        encodedpackages.append(encode_transaction)

    print 'Item index is ', itemIndx[test_str]
    # encodedpackages: ([Features, app])
    return encodedpackages, rever_map(appIndx), rever_map(itemIndx), packageHost

def _gen_rules(transactions, tSupport, tConfidence, featureIndx):
    '''
    Generate encoded rules
    Input
    - transactions : encoded transaction
    - tSupport    : frequent pattern support
    - tConfidence : frequent pattern confidence
    - featureIndx : a map between number and feature string
    Return : (itemsets, confidencet, support, label)
    '''
    ###########################
    # FP-tree Version
    ###########################
    print '_gen_rules', featureIndx.keys()[:10]
    rules = []
    for frequent_pattern_info in find_frequent_itemsets(transactions, tSupport, True):
        itemset, support, tag_dist = frequent_pattern_info
        max_clss = max(tag_dist.iteritems(), key=operator.itemgetter(1))[0]
        if tag_dist[max_clss] * 1.0 / support > tConfidence:
            confidence = max(tag_dist.values()) * 1.0 / support
            rules.append((frozenset(itemset), confidence, support, max_clss))
    
    print ">>> Finish Rule Generating"
    return rules

def _remove_duplicate(t_rules):

  root ={'rule': None, 'child':{}, 'label':None, 'support':0, 'confidence':0}
  new_rules = []
  for rule in t_rules:
    node = root
    pruned = False
    for item in rule[0]:
      if item in node['child'] and node['child'][item]['label'] != None:
        pruned = True
        break
      else:
          new_node ={'rule':item, 'child':{}, 'label':None, 'support':0, 'confidence':0}
          node['child'][item] = new_node
      node = node['child'][item]

    if not pruned:
      node['label'] = rule[3]
      node['support'] = rule[2]
      node['confidence'] = rule[1]
      new_rules.append(rule)
  print 'original size', len(t_rules), 'new size', len(new_rules)
  return new_rules

def _prune_rules(t_rules, packages, min_cover = 3):
    '''
    Input t_rules: ( rules, confidence, support, class_label ), get from _gen_rules
    Input packages: list of packets
    Return: (rule, label, package_id, confidence, support)
    '''
    import datetime
    ts = datetime.datetime.now()

    # Sort generated rules according to its confidence, support and length
    t_rules.sort(key=lambda v: (v[1], v[2], len(v[0])), reverse=True)
    t_rules = _remove_duplicate(t_rules)

    cover_num = defaultdict(int)
    package_ids = {frozenset(package):i  for i, package in enumerate(packages)}
    index_packages = defaultdict(list)
    # Change packages to sets
    map(lambda package: index_packages[package[-1]].append(frozenset(package)), packages)
    packages = index_packages
    
    for rule, confidence, support, classlabel in t_rules:
        for package in packages[classlabel]:
            if cover_num[package] <= min_cover and rule.issubset(package):
                cover_num[package] += 1
                yield Rule(rule, classlabel, package_ids[package], confidence, support)
    print ">>> Pruning time:", (datetime.datetime.now() - ts).seconds


def _persist(rules, rule_type):
    sqldao = SqlDao()
    QUERY = consts.SQL_INSERT_CMAR_RULES
    params = []
    for rule in rules:
        params.append((rule.label, ','.join(rule.rule), rule.confidence, rule.support, rule.host, rule_type))
    sqldao.executeBatch(QUERY, params)
    sqldao.close()
    print "Total Number of Rules is", len(rules)

class CMAR:
  def __init__(self, min_cover=3):
    # feature, app, host
    self.rules = defaultdict(lambda : defaultdict(lambda : defaultdict()))
    self.min_cover = min_cover

  def train(self, packages, rule_type, tSupport=2, tConfidence=0.8):
      p = []
      for tbl_packages in packages.values():
        p += tbl_packages
      packages = p
      print "#CMAR:", len(packages)
      encodedpackages, appIndx, featureIndx, packageHost = _encode_data(packages)
      # Rules format : (feature, confidence, support, label)
      rules = _gen_rules(encodedpackages, tSupport, tConfidence, featureIndx)
      # feature, app, host
      rules = _prune_rules(rules, encodedpackages, self.min_cover)
      # change encoded features back to string
      decodedRules = set()
      tmp = set()
      for rule in rules:
          rule_str = frozenset({featureIndx[itemcode] for itemcode in rule[0]})
          charactor = {packageHost[rule.host]}
          charactor.add(rule_str)
          # if charactor not in tmp:
          tmp.add(frozenset(charactor))
          decodedRules.add(Rule(rule_str, appIndx[rule.label], packageHost[rule.host], rule.confidence, rule.support))

      self._add_rules(decodedRules, rule_type)
      _persist(decodedRules, rule_type)
      self.__init__()
      return self

  def _add_rules(self, rules, ruleType):
      tmprules = {}
      for feature, app, host, confidence, _ in rules:
          tmprules.setdefault(host, {})
          tmprules[host][feature] = (app, confidence)
      self.rules[ruleType] = tmprules

  def load_rules(self):
    self.rules = {}
    self.rules[consts.APP_RULE] = defaultdict(lambda : defaultdict())
    self.rules[consts.COMPANY_RULE] = defaultdict(lambda : defaultdict())
    self.rules[consts.CATEGORY_RULE] = defaultdict(lambda : defaultdict())
    sqldao = SqlDao()
    counter = 0
    SQL = consts.SQL_SELECT_CMAR_RULES
    for label, patterns, host, rule_type, confidence in sqldao.execute(SQL):
      counter += 1
      patterns = frozenset(map(lambda x: x.strip(), patterns.split(",")))
      self.rules[rule_type][host][patterns] = (label, confidence)
    sqldao.close()
    print '>>>[CMAR] Totaly number of rules is', counter
    for rule_type in self.rules:
      print '>>>[CMAR] Rule Type %s Number of Rules %s' % (rule_type, len(self.rules[rule_type]))
    

  def _clean_db(self, rule_type):
    sqldao = SqlDao()
    sqldao.execute( consts.SQL_DELETE_CMAR_RULES % (rule_type))
    sqldao.commit()
    sqldao.close()

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
    return labelRsts
