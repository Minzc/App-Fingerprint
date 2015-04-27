from nltk import FreqDist

from sqldao import SqlDao

from utils import Relation, load_pkgs, loadfile
from collections import defaultdict
import operator


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

  def _gen_rules(self, serviceKey, confidence, support):
    root = self._root
    hostnodes = root.children.values()
    rules = []
    for hostNode in hostnodes:
        hostName = hostNode.value
        for tokenNode in hostNode.children.values():
            tokenName = tokenNode.value
            validToken = False
            # number of app's use this token
            tokenSupport = len(tokenNode.children)
            tokenConfidence = 1.0 * tokenNode.count / hostNode.count
            print 'DEBUG', hostNode.value.encode(
                'utf-8'), hostNode.count, 'tokensupport:', tokenSupport, 'tokenConfidence:', tokenConfidence, tokenName.encode(
                'utf-8')

            if tokenNode.status == 1:
                # check token confidence and token support
                # if tokenConfidence < confidence or tokenSupport < support:
                if tokenSupport < support:
                    continue
                validToken = True

            if hostName in serviceKey.get() and tokenName in serviceKey.get()[
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

    for appnode in appnodes: 
      if len(appnode.children) > 1: 
        for tokennode in appnode.father: 
          tokennode.set_status(-1)



class ParamRules:
  def __init__(self):
    def _dictvalue_factory():
      return defaultdict(int)
    self.results = defaultdict(_dictvalue_factory)
    self._rules = []
    def load_rules(ln): 
      host, rule = ln.split(':')
      rule = [host] + filter(None, rule.split('$'))
      print rule
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
      print ">>> Pattern : ", pattern
      company_rules[pattern] = defaultdict(int)
      company_rules[pattern][company] += 1
      app_rules[pattern] = defaultdict(int)
      app_rules[pattern][app] += 1
    for k, company_dist in company_rules.iteritems():
      company = max(company_dist.iteritems(), key=operator.itemgetter(1))[0]
      support = company_dist[company]
      confidence = support * 1.0 / sum(company_dist.values())
      print k[:-1], k[-1], 'confidence:', confidence
      yield (k[:-1], company, k[-1], confidence, support)
    for k, app_dist in app_rules.iteritems():
      app = max(app_dist.iteritems(), key=operator.itemgetter(1))[0]
      support = app_dist[app]
      confidence = support * 1.0 / sum(app_dist.values())
      print k[:-1], k[-1], 'confidence:', confidence
      yield (k[:-1], app, k[-1], confidence, support)

  def load_rules(self):
    sqldao = SqlDao()
    self.rules = defaultdict(dict)
    QUERY = 'SELECT kvpattern, host, label FROM patterns WHERE kvpattern IS NOT NULL'
    for kv, host, label in sqldao.execute(QUERY):
      self.rules[host][frozenset(kv.split('&'))] = label

  def classify(self, package):
    kv = { k+'='+v[0] for k, v in package.querys.iteritems()}
    host = package.secdomain
    rst = set()
    for k, v in self.rules[host].iteritems():
      if k.issubset(kv):
        rst.add(v)
    for k,v in self.rules[''].iteritems():
      if k.issubset(kv):
        rst.add(v)
    return rst

def KVMiner(training_data=None, confidence=0.8, support=2): 
  sqldao = SqlDao()
  sqldao.execute('DELETE FROM patterns WHERE kvpattern IS NOT NULL')

  training_data = load_pkgs() if not training_data else training_data

  tree = KVTree()
  serviceKey = Relation()

  # Build the forest
  for package in training_data:
    tree._build_tree(package, package.querys.items())
    for k, v in package.querys.items():
      if package.app in v or package.name in v:
        serviceKey.add(package.secdomain, k)
  print 'DEBUG', serviceKey
  
  tree._prune_forest()

  rules = tree._gen_rules(serviceKey, confidence, support)

  # Persist
  sqldao = SqlDao()
  # app, value, token, host
  for appName, appCompany, valueName, tokenName, hostName, tokenConfidence, tokenSupport in rules:
      sqldao.execute(
          'insert into features (host, company, token, app, value, confidence, support) values(%s,%s,%s,%s,%s,%s,%s)',
          (hostName, appCompany, tokenName, appName, valueName, tokenConfidence, tokenSupport))

  sqldao.close()


def _buildTestTree():
    sqldao = SqlDao()
    query = 'SELECT app, company, token, host, value FROM features'
    tree = KVTree()
    vnodes = {}

    # Build the tree
    for app, company, token, host, value in sqldao.execute(query):
        treePath = (host, token, value, app)
        tree._add_node( treePath )

    sqldao.close()
    return tree._root


def _test(package, root):
    hst = package.secdomain
    app = package.app
    company = package.company
    predict_app = None

    if hst:
        predict = None
        for key, values in package.querys.items():
            for value in filter(None, values):
                treePath = (hst, key, value)
                print ">>> [DEBUG] treePath is", treePath
                treeNode = root
                while len(treePath) > 0 and treeNode != None:
                    treeNode = treeNode.search(treePath[0])
                    treePath = treePath[1:]
                # treeNode is valuenode
                if treeNode:
                    appNodes = treeNode.children.values()
                    print appNodes, treeNode.value
                    if len(appNodes) == 1:
                        predict = (appNodes[0], None)
                        predict_app = (appNodes[0].value, None)

        if predict:
            # predict company
            if not predict[1]:
                if predict[1] != package.app:
                    predict = predict[0]
                    print predict.value, app
                    value = predict.father[0].value
                    token = predict.father[0].father[0].value
                    host = predict.father[0].father[0].father[0].value
                    print "rules: %s\t%s\t%s" % (value, token, host)
            # predict app
            else:
                predict = predict[0]
                if predict.get_value() != package.company:
                    print predict.get_value(), app
                    value = predict.get_father()[0].get_value()
                    token = predict.get_father()[0].get_father()[0].get_value()
                    host = predict.get_father()[0].get_father()[0].get_father()[0].get_value()
                    print "rules: %s\t%s\t%s" % (value, token, host)
    # DEBUG
    return predict_app


def test_algo(test_set=None):
    import urlparse

    root = _buildTestTree()
    correct = 0
    wrong = 0
    correct_ids = []

    if not test_set:
        test_set = load_pkgs()

    rst = {}
    for package in test_set:
        predictRst = _test(package, root)
        if not predictRst and package.refer:
            tmpHost = urlparse.urlparse(package.refer).netloc
            package.set_host(tmpHost)
            tmpQuery = urlparse.urlparse(package.refer).query
            tmpPath = urlparse.urlparse(package.refer).path
            package.set_path(tmpPath + '?' + tmpQuery)
            predictRst = _test(package, root)

        if predictRst:
            predict_app, predict_company = predictRst
            rst[package.id] = predictRst
            if predict_app == package.app or predict_company == package.company:
                correct += 1
                correct_ids.append(package.id)
            else:
                wrong += 1

    upquery = "update packages set classified = %s where id = %s"
    sqldao = SqlDao()
    for pid in correct_ids:
        sqldao.execute(upquery, (10, pid))
    _sqldao.close()

    print 'Total:%s\tCorrect:%s\tWrong:%s' % (len(test_set), correct, wrong)
    return rst


def test_rule_miner_train():
  def tuples2str(tuples):
    return '&'.join([k+'='+v for k, v in tuples ])
  paraminer = ParamRules()
  patterns = []
  print ">>> Starting processing packages"
  counter = 0
  for record in load_pkgs():
    subpatterns = list(paraminer.mine(record))
    patterns += subpatterns
    if len(subpatterns) > 0:
      counter += 1
  sqldao = SqlDao()
  sqldao.execute('DELETE FROM patterns WHERE kvpattern IS NOT NULL')
  QUERY = 'INSERT INTO patterns (label, support, confidence, host, kvpattern) VALUES (%s, %s, %s, %s, %s)'
  for recog_rule in paraminer.stat(patterns):
    k, app, host, confidence, support = recog_rule
    k = tuples2str(k)
    sqldao.execute(QUERY, (app, support, confidence, host, k))
    print k, app, host, confidence, support
  print counter

def test_rule_miner_test():
  paraminer = ParamRules()
  paraminer.load_rules()
  count = 0
  for record in load_pkgs():
    if len(paraminer.classify(record)) > 0:
      count += 1
  print count


import sys

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print 'Number of args is wrong'
    elif sys.argv[1] == 'test':
        test_algo()
    elif sys.argv[1] == 'train':
        KVMiner()
    elif sys.argv[1] == 'train_rules':
      test_rule_miner_train()
    elif sys.argv[1] == 'test_rules':
      test_rule_miner_test()
