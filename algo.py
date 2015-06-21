from nltk import FreqDist
import consts
from sqldao import SqlDao

from utils import Relation, load_pkgs, loadfile
from collections import defaultdict
import operator


DEBUG = False

class TreeNode:
  def __init__(self, father, value):
    self._children = {}
    self._father = [father]
    self._value = value
    self._status = 1
    self._count = 0
    self._leaf = {}

  def inc(self):
    self._count += 1

  @property
  def count(self):
    """Number of times this node's value occurred in data"""
    return self._count

  @property
  def value(self):
    """Value stored in the node"""
    return self._value

  def add(self, child):
    """Add a child to the node"""
    self._children[child.value] = child

  def search(self, value):
    """
    Check to see if this node contains a child node for the given item
    if so, that node is returned; otherwise, `None` is retuerned
    """
    return self._children[value] if value in self._children else None

  @property
  def children(self):
    """return all children of that node"""
    return self._children

  @property
  def father(self):
    return self._father

  def set_status(self, status):
    self._status = status
  
  @property
  def status(self):
    return self._status

  def to_string(self):
    return ','.join([child._value for child in self._children])
  
  def add_leaf(self, node):
    self._leaf[node._value] = node

  def leaf(self):
    return self._leaf

  def add_father(self, node):
    self._father.append(node)

class KVTree:
  def __init__(self):
    self._root = TreeNode(None, None)
    self._vnodeIndex = {}
    self._appnodes = []
    

  def _add_node(self, treePath, addInfo=None):
    tree_node = self._root
    host = treePath[0]
    token = treePath[1]
    appnode = None
    valuenode = None
    for i, node_value in enumerate(treePath):
      child_node = tree_node.search(node_value)
      # not adding leaf node
      if not child_node:
        # not leaf node
        if i == 0 or i == 1 or i == 2:
          child_node = TreeNode(tree_node, node_value)
          tree_node.add(child_node)
        elif i == 3:  # value node shared among the forest
          child_node = self._vnodeIndex.get(node_value, None)
          if not child_node:
            child_node = TreeNode(tree_node, node_value)
            self._vnodeIndex[node_value] = child_node 
          else: 
            child_node.add_father(tree_node) 
          tree_node.add(child_node)

      child_node.inc()
      tree_node = child_node
      if i == 3: 
        valuenode = tree_node
      elif i == 2: 
        appnode = tree_node
    self._appnodes.append(appnode)


  def _build_tree(self, package, kvs): 
    hst = package.secdomain if package.secdomain else package.dst

    addInfo = {}
    addInfo['company'] = package.company if package.company else package.app

    if hst:
      for key, values in kvs: 
        for value in filter(None, values): 
          treePath = (hst, key, package.app, value) 
          self._add_node(treePath, addInfo) 

  def _gen_rules(self, goodCandidates, confidence, support):
    hostnodes = self._root.children.values()
    rules = []
    for hostNode in hostnodes:
        hostName = hostNode.value

        for tokenNode in hostNode.children.values():
            tokenName = tokenNode.value

            validToken = False
            # number of app's using this token
            tokenSupport = len(tokenNode.children)
            tokenConfidence = 1.0 * tokenNode.count / hostNode.count
            if DEBUG : print '>>> [DEBUG:gen_rules]', hostNode.value.encode(
                'utf-8'), hostNode.count, 'tokensupport:', tokenSupport, 'tokenConfidence:', tokenConfidence, tokenName.encode(
                'utf-8')

            if tokenNode.status == 1:
                # check if the pattern can satify the support and confidence requirement
                # if tokenConfidence >= confidence and tokenSupport >= support:
                validToken = True

            if hostName in goodCandidates and tokenName in goodCandidates[
                hostName] and tokenNode.status != -1:
                validToken = True

            if validToken:
              for appNode in tokenNode.children.values():
                    appName = appNode.value
                    # appCompany = appNode.addInfo['company']
                    appCompany = 'Test'
                    for valueNode in appNode.children.values():
                        # print 'value:', valuenode.value, 'fathernum:',len(valuenode.father)
                        valueName = valueNode.value
                        appCount = appNode.count
                        rules.append(
                            (appName, appCompany, valueName, tokenName, hostName, tokenConfidence, tokenSupport))
    return rules

  def _prune_forest(self): 
    vnodes = self._vnodeIndex
    appnodes = self._appnodes
    # Prune
    for vnode in vnodes.values(): 
      appvalues = set() 
      for appnode in vnode.father: 
        appvalues.add(appnode.value) 

       # keep one app or one company
        if len(appvalues) > 1:
          for appnode in vnode.father: 
            appnode.father[0].set_status(0)
            if appnode.father[0].value == 'client':
              print "1", vnode.value

    for appnode in appnodes: 
      if len(appnode.children) > 1: 
        for tokennode in appnode.father:
          tokennode.set_status(-1)
          if tokennode.value == 'client':
            print "2", appnode.value
            for vnode in appnode.children:
                print vnode



class ParamRules:
  def __init__(self):
    def _dictvalue_factory():
      return defaultdict(int)
    self.results = defaultdict(_dictvalue_factory)
    self._rules = []
    def load_rules(ln): 
      host, rule = ln.split(':')
      rule = [host] + filter(None, rule.split('$'))
      self._rules.append(rule)

    loadfile('ad_dict.txt', load_rules)
  
  def mine(self, package):
    for rule in self._rules:
      if rule[0] in package.host:
        match = True
        pattern = []
        for key in rule[1:]:
          if key in package.querys:
            pattern.append((key, package.querys[key][0]))
          elif package.form and key in package.form:
            pattern.append((key, package.form[key]))
          else:
            match = False
        if match:
          host = package.secdomain if rule[0] != '' else ''
          pattern.append(host)
          pattern.append(package.company)
          pattern.append(package.app)
          yield pattern
  
  def stat(self, patterns):
    company_rules = {}
    app_rules = {}
    for pattern in patterns:
      host, company, app = pattern[-3], pattern[-2], pattern[-1]
      pattern = tuple(pattern[:-2])
      if DEBUG : print ">>> Pattern : ", pattern
      company_rules[pattern] = defaultdict(int)
      company_rules[pattern][company] += 1
      app_rules[pattern] = defaultdict(int)
      app_rules[pattern][app] += 1
    for k, company_dist in company_rules.iteritems():
      company = max(company_dist.iteritems(), key=operator.itemgetter(1))[0]
      support = company_dist[company]
      confidence = support * 1.0 / sum(company_dist.values())
      if DEBUG : print '>>> [DEBUG:stat]', k[:-1], k[-1], 'confidence:', confidence
      yield (k[:-1], company, k[-1], confidence, support, consts.COMPANY_RULE)
    for k, app_dist in app_rules.iteritems():
      app = max(app_dist.iteritems(), key=operator.itemgetter(1))[0]
      support = app_dist[app]
      confidence = support * 1.0 / sum(app_dist.values())
      if DEBUG: print k[:-1], k[-1], 'confidence:', confidence
      yield (k[:-1], app, k[-1], confidence, support, consts.APP_RULE)

  def load_rules(self):
    sqldao = SqlDao()
    self.rules = {}
    self.rules[consts.APP_RULE] = defaultdict(dict)
    self.rules[consts.COMPANY_RULE] = defaultdict(dict)
    QUERY = 'SELECT kvpattern, host, label, confidence,rule_type FROM patterns WHERE kvpattern IS NOT NULL'
    for kv, host, label, confidence, rule_type in sqldao.execute(QUERY):
      self.rules[rule_type][host][frozenset(kv.split('&'))] = (label, confidence)

  def classify(self, package):
    kv = { k+'='+v[0] for k, v in package.querys.iteritems()}
    if package.form:
      for k,v in package.form.iteritems():
        kv.add(k + '=' + v)

    # Format testing data
    host = package.secdomain
    rst = defaultdict(list)
    for rulesID, rules in self.rules.iteritems():
      for k, v in rules[host].iteritems():
        if k.issubset(kv):
          rst[rulesID].append(v)
      for k,v in rules[''].iteritems():
        if k.issubset(kv):
          rst[rulesID].append(v)

    if len(rst) == 0:
      # use predefined rules to classify
      patterns = list(self.mine(package))
      for pattern in patterns:
        for k,v in pattern[:-3]:
          if ' ' not in v and '.' in v:
            rst[consts.APP_RULE].append((v, 1.0))
    return rst



def _persist(patterns, paraminer, tree, goodCandidates):
    def tuples2str(tuples):
      return '&'.join([k+'='+v for k, v in tuples ])
    QUERY = 'INSERT INTO patterns (label, support, confidence, host, kvpattern, rule_type) VALUES (%s, %s, %s, %s, %s, %s)'
    sqldao = SqlDao()
    # Param rules
    params = []
    for recog_rule in paraminer.stat(patterns):
      k, app, host, confidence, support, rule_type = recog_rule
      k = tuples2str(k)
      params.append((app, support, confidence, host, k, rule_type))
    sqldao.executeBatch(QUERY, params)
    # Tree rules
    params = []
    for appName, appCompany, valueName, tokenName, hostName, tokenConfidence, tokenSupport in tree._gen_rules(goodCandidates, confidence, support):
        params.append((appName, tokenSupport, 1.0, hostName, tokenName+'='+valueName, consts.APP_RULE))
    sqldao.executeBatch(QUERY , params)
    sqldao.close()


class KVClassifier:
  def __init__(self):
    self.paraminer = ParamRules()
    
  def _clean_db(self):
    sqldao = SqlDao()
    sqldao.execute('DELETE FROM patterns WHERE kvpattern IS NOT NULL')
    sqldao.commit()
    sqldao.close()

  def train(self, training_data=None, confidence=0.8, support=2): 

    self._clean_db()
    training_data = load_pkgs() if not training_data else training_data

    patterns = []

    tree = KVTree()
    goodCandidates = defaultdict(lambda : defaultdict(int))
    
    #############
    # Mining
    #############
    for package in training_data:
      subpatterns = list(self.paraminer.mine(package))
      patterns += subpatterns
      # Build the forest
      kvs = package.querys.copy()
      if package.form : 
        for k,v in package.form.iteritems():
          kvs[k] = [v]

      tree._build_tree(package, kvs.items())

      for k, v in kvs.items():
        if package.app in v or package.name in v:
          goodCandidates[package.secdomain][k] += 1


    if DEBUG : print '>>> [DEBUG:kvminer:goodCandidate]', goodCandidates
    
    tree._prune_forest()

    ###############
    # Persist
    ###############
    _persist(patterns, self.paraminer, tree, goodCandidates)
    self.paraminer.load_rules()
    return self.paraminer
  
  def classify(self, record):
    return self.paraminer.classify(record)



def KVPredictor(test_records=None):
  paraminer = ParamRules()
  paraminer.load_rules()
  count = 0
  correct = 0
  if not test_records: test_records = load_pkgs()
  else: test_records = test_records.values()
  rst = {}
  for record in test_records:
    predictRst = paraminer.classify(record)
    if len(predictRst) > 0:
      count += 1
      if (record.app in predictRst):
        correct += 1
      rst[record.id] = predictRst.pop()
  print ">>> Correct:", correct, "Total:", count, "Precision:", correct * 1.0 / count
  return rst


import sys

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print 'Number of args is wrong'
    elif sys.argv[1] == 'test':
      KVPredictor()
    elif sys.argv[1] == 'train':
      KVMiner()
