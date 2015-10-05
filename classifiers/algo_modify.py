import const.consts as consts
import re
from sqldao import SqlDao
from utils import load_exp_app, suffix_tree
from collections import defaultdict, namedtuple
from classifier import AbsClassifer


DEBUG = False

class KVClassifier(AbsClassifer):
  def __init__(self, appType):
    self.name = consts.KV_CLASSIFIER
    self.featureTbl = defaultdict(lambda : defaultdict( lambda : defaultdict( lambda : defaultdict(set))))
    self.valueLabelCounter = defaultdict(set)
    self.rules = {}
    exp_apps = load_exp_app()
    self.appRegex = {}
    for appName in exp_apps[appType]:
      if '.' in appName:
        self.appRegex[appName] = re.compile(appName)
      else:
        self.appRegex[appName] = re.compile('='+appName)

    self.appSuffix = suffix_tree(exp_apps[appType])
    self.appType = appType
  def prune_specific_rules(self, specificRules, trainData):
    '''
    Pruning redundant keys
    Input
    - generalRules : {secdomain: [Rule(secdomain, key, score, labelNum)]}
    - trainData : {tbl: [pkgs]}
    '''
                # specificRules[pkg.host][rule.key][value][pkg.label][consts.SCORE] = rule.score
                # specificRules[pkg.host][rule.key][value][pkg.label][consts.SUPPORT].add(tbl)
    print 'Start Pruning'
    PKG_IDS = 1
    VALUES = 2
    newSpecificRules = defaultdict(lambda : defaultdict( lambda : defaultdict( lambda : defaultdict(lambda : {PKG_IDS:{},VALUES:set()}))))
    recorder = defaultdict(lambda : defaultdict(set))
    scores = defaultdict(lambda : defaultdict(set))
    for tbl, pkgs in trainData.iteritems():
      for pkg in pkgs:
        host = pkg.host
        for key in pkg.queries:
          for value in pkg.queries[key]:
            if pkg.label in specificRules[host][key][value]:
              # Here we can change it to rule score
              ruleScore = specificRules[host][key][value][pkg.label][consts.SUPPORT]
              scores[host][key] = specificRules[host][key][value][pkg.label][consts.SCORE]
              recorder[host][key].add(tbl+'#'+str(pkg.id))
    reversPkgids = defaultdict(lambda : defaultdict(set))
    for host in recorder:
      for key in recorder[host]:
        for pkgSet in recorder[host][key]:
          reversPkgids[host][frozenset(pkgSet)].add((host,key, scores[host][key], len(pkgSet)))

    for host, pkgIdNrules in reversPkgids.iteritems():
      finalTuples = []
      pkgIdNrules = pkgIdNrules.items()
      # pkgIdNrules : (pkgIds, {(host, key, score, len_pkg_set), rule, rule}), (....), ...
      pkgIdNrules = sorted(pkgIdNrules, key=lambda x: len(x[0]),reverse=True)
      for pkgIdNrule in pkgIdNrules:
        ifPut = True
        for finalTuple in finalTuples:
          if pkgIdNrule[0].issubset(finalTuple):
              ifPut = False
              finalTuple[1] += finalTuple[1]
        if ifPut:
          finalTuples.append(pkgIdNrule)
            
      for pkgIds, rules in finalTuples:
          print host, rules
          print '=' * 10

  def train(self, trainData, rule_type):
    for tbl in trainData.keys():
      for pkg in trainData[tbl]:
        for k,v in pkg.queries.items():
          if pkg.secdomain == 'bluecorner.es' or pkg.host == 'bluecorner.es' or pkg.label == 'com.bluecorner.totalgym':
            #print 'OK contains bluecorner', pkg.secdomain
            pass
          map(lambda x : self.featureTbl[pkg.secdomain][pkg.label][k][x].add(tbl), v)
          map(lambda x : self.valueLabelCounter[x].add(pkg.label), v)
    ##################
    # Count
    ##################
    # secdomain -> app -> key -> value -> tbls
    # secdomain -> key -> (label, score)
    keyScore = defaultdict(lambda : defaultdict(lambda : {consts.LABEL:set(), consts.SCORE:0}))
    for secdomain in self.featureTbl:
      for label in self.featureTbl[secdomain]:
        for k in self.featureTbl[secdomain][label]:
          for v, tbls in self.featureTbl[secdomain][label][k].iteritems():
            if secdomain == 'bluecorner.es':
              pass
            if len(self.valueLabelCounter[v]) == 1:
              cleanedK = k.replace("\t", "")
              keyScore[secdomain][cleanedK][consts.SCORE] += \
                (len(tbls) - 1) / float(len(self.featureTbl[secdomain][label][k]))
              keyScore[secdomain][cleanedK][consts.LABEL].add(label)
    #############################
    # Generate interesting keys
    #############################
    Rule = namedtuple('Rule', 'secdomain, key, score, labelNum')
    generalRules = defaultdict(list)
    for secdomain in keyScore:
      for key in keyScore[secdomain]:
        labelNum = len(keyScore[secdomain][key][consts.LABEL]) 
        score = keyScore[secdomain][key][consts.SCORE]
        if labelNum == 1 or score == 0:
          continue
        generalRules[secdomain].append(Rule(secdomain, key, score, labelNum))
    for secdomain in generalRules:
      generalRules[secdomain] = sorted(generalRules[secdomain], key=lambda rule: rule.score, reverse = True)

    print ">>>[KV] generalRules", len(generalRules)
    #############################
    # Generate specific rules
    #############################
    specificRules = defaultdict(lambda : defaultdict( lambda : defaultdict( lambda : defaultdict(lambda : {consts.SCORE:0,consts.SUPPORT:set()}))))
    
    for tbl, pkgs in trainData.iteritems():
      for pkg in filter(lambda pkg : pkg.secdomain in generalRules, pkgs):
        for rule in filter(lambda rule : rule.key in pkg.queries, generalRules[pkg.secdomain]):
          for value in pkg.queries[rule.key]:
            value = value.strip()
            if len(self.valueLabelCounter[value]) == 1 and len(value) != 1:
                specificRules[pkg.host][rule.key][value][pkg.label][consts.SCORE] = rule.score
                specificRules[pkg.host][rule.key][value][pkg.label][consts.SUPPORT].add(tbl)
    self.prune_specific_rules(specificRules, trainData)
    #############################
    # Persist rules
    #############################
    self.persist(specificRules, rule_type)
    self.__init__(self.appType)
    return self

  def _clean_db(self, rule_type):
    print '>>> [KVRULES]', consts.SQL_DELETE_KV_RULES % rule_type
    sqldao = SqlDao()
    sqldao.execute(consts.SQL_DELETE_KV_RULES % rule_type)
    sqldao.commit()
    sqldao.close()

  def load_rules2(self):
    self.rules = {}
    sqldao = SqlDao()
    self.rules[consts.APP_RULE] = defaultdict(lambda : defaultdict( lambda : defaultdict( lambda : defaultdict(lambda : {'score':0, 'support':0}))))
    self.rules[consts.COMPANY_RULE] = defaultdict(lambda : defaultdict( lambda : defaultdict( lambda : defaultdict(lambda : {'score':0, 'support':0}))))
    self.rules[consts.CATEGORY_RULE] = defaultdict(lambda : defaultdict( lambda : defaultdict( lambda : defaultdict(lambda : {'score':0, 'support':0}))))
    QUERY = consts.SQL_SELECT_KV_RULES
    counter = 0
    for key, value, host, label, confidence, rule_type, support in sqldao.execute(QUERY):
      counter += 1
      self.rules[rule_type][host][key][value][label][consts.SCORE] = confidence
      self.rules[rule_type][host][key][value][label][consts.SUPPORT] = support
    print '>>> [KV Rules#Load Rules] total number of rules is', counter
    sqldao.close()

  def load_rules(self):
    self.rules = {}
    sqldao = SqlDao()
    self.rules[consts.APP_RULE] = defaultdict(lambda : defaultdict( lambda : defaultdict( lambda : defaultdict(lambda : {'score':0, 'support':0, 'regexObj': None}))))
    self.rules[consts.COMPANY_RULE] = defaultdict(lambda : defaultdict( lambda : defaultdict( lambda : defaultdict(lambda : {'score':0, 'support':0, 'regexObj': None}))))
    self.rules[consts.CATEGORY_RULE] = defaultdict(lambda : defaultdict( lambda : defaultdict( lambda : defaultdict(lambda : {'score':0, 'support':0, 'regexObj': None}))))
    QUERY = consts.SQL_SELECT_KV_RULES
    counter = 0
    for key, value, host, label, confidence, rule_type, support in sqldao.execute(QUERY):
      if len(value.split('\n')) == 1:
        counter += 1
        self.rules[rule_type][host][key][value][label][consts.SCORE] = confidence
        self.rules[rule_type][host][key][value][label][consts.SUPPORT] = support
        self.rules[rule_type][host][key][value][label][consts.REGEX_OBJ] = re.compile(re.escape(key+'='+value))
    print '>>> [KV Rules#Load Rules] total number of rules is', counter
    sqldao.close()

  def classify(self, pkg):
    predictRst = {}
    for ruleType in self.rules:
      for host, queries in [(pkg.host, pkg.queries)]:
        fatherScore = -1
        rst = consts.NULLPrediction

        for k, kRules in self.rules[ruleType].get(host, {}).iteritems():
          for v in queries.get(k, []):          
            for label, scoreNcount in kRules.get(v, {}).iteritems():
              score, support, regexObj = scoreNcount[consts.SCORE], scoreNcount[consts.SUPPORT], scoreNcount[consts.REGEX_OBJ]
              match = regexObj.search(pkg.host)

              if support > rst.score or (support == rst.score and score > fatherScore):
                fatherScore = score
                evidence = (k, v)
                rst = consts.Prediction(label, support, evidence)

        predictRst[ruleType] = rst
        
        # If we can not predict based on kv in urls, use suffix tree to try again
        if not predictRst[consts.APP_RULE].label:
          for appName, regexObj in self.appRegex.iteritems():
            match = regexObj.search(pkg.origPath)
            if match:
              predictRst[consts.APP_RULE]= consts.Prediction(appName, 1, ('ORIGINAL_PATH', appName))

        # If we can predict based on original url, we do not need to use refer url to predict again
        if predictRst[ruleType].label != None:
          break
    if predictRst[consts.APP_RULE] != consts.NULLPrediction and predictRst[consts.APP_RULE].label != pkg.app:
      print predictRst[consts.APP_RULE].evidence, predictRst[consts.APP_RULE].label, pkg.app
      print '=' * 10
    return predictRst
  def classify2(self, pkg):
    predictRst = {}
    for ruleType in self.rules:
      for host, queries in [(pkg.host, pkg.queries), (pkg.refer_host, pkg.refer_queries)]:
        fatherScore = -1
        rst = consts.NULLPrediction

        for k, kRules in self.rules[ruleType].get(host, {}).iteritems():
          for v in queries.get(k, []):          
            for label, scoreNcount in kRules.get(v, {}).iteritems():
              score, support = scoreNcount[consts.SCORE], scoreNcount[consts.SUPPORT]

              if support > rst.score or (support == rst.score and score > fatherScore):
                fatherScore = score
                evidence = (k, v)
                rst = consts.Prediction(label, support, evidence)

        predictRst[ruleType] = rst
        
        # If we can not predict based on kv in urls, use suffix tree to try again
        if not predictRst[consts.APP_RULE].label:
          for k, values in queries.iteritems():
            label = map(lambda v : self.classify_suffix_app(v), values)
            if label[0] != None:
              predictRst[consts.APP_RULE] = consts.Prediction(label[0], 1, (k, label[0]))

        # If we can predict based on original url, we do not need to use refer url to predict again
        if predictRst[ruleType].label != None:
          break
    return predictRst

  def classify_suffix_app(self, value):
    value = value.split('.')
    node = self.appSuffix
    meet_first = False
    rst = []
    for i in reversed(value):
      if not meet_first and i in node.children:
        meet_first = True
      if meet_first:
        if i in node.children:
          rst.append(i)
          node = node.children[i]
        if len(node.children) == 0:
          return '.'.join(reversed(rst))
    return None

 

  def persist(self, patterns, rule_type):
    self._clean_db(rule_type)
    QUERY = consts.SQL_INSERT_KV_RULES
    sqldao = SqlDao()
    # Param rules
    params = []
    for host in patterns:
      for key in patterns[host]:
        for value in patterns[host][key]:
          max_confidence = -1
          max_support = -1
          max_label = None
          for label in patterns[host][key][value]:
            confidence = patterns[host][key][value][label][consts.SCORE]
            support = len(patterns[host][key][value][label][consts.SUPPORT])
            params.append((label, support, confidence, host, key, value, rule_type))
    sqldao.executeBatch(QUERY, params)
    sqldao.close()
    print ">>> [KVRules] Total Number of Rules is %s Rule type is %s" % (len(params), rule_type)


import sys

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print 'Number of args is wrong'
    elif sys.argv[1] == 'test':
      KVPredictor()
    elif sys.argv[1] == 'train':
      KVMiner()




  # def load_rules_without_key(self):
  #   print '>>> [KVClassifier] load rules'
  #   sqldao = SqlDao()
  #   self.rules[consts.APP_RULE] = defaultdict(lambda : defaultdict( lambda : defaultdict( lambda : defaultdict(lambda : {'score':0, 'support':0}))))
  #   self.rules[consts.COMPANY_RULE] = defaultdict(lambda : defaultdict( lambda : defaultdict( lambda : defaultdict(lambda : {'score':0, 'support':0}))))
  #   QUERY = 'SELECT paramkey, paramvalue, host, label, confidence, rule_type, support FROM patterns WHERE paramkey IS NOT NULL'
  #   counter = 0
  #   for key, value, host, label, confidence, rule_type, support in sqldao.execute(QUERY):
  #     counter += 1
  #     self.rules[rule_type][host][value][label]['score'] = confidence
  #     self.rules[rule_type][host][value][label]['support'] = support
  #   print counter
  #   
  #   
  # def classify_without_key(self, pkg):
  #   maxScore = -1
  #   occurCount = -1
  #   predict_app = None
  #   tmp_rst = None
  #   secdomain = ''
  #   if secdomain in self.rules[consts.APP_RULE]:
  #     for k in pkg.queries:
  #       for v in pkg.queries[k]:
  #         if v.lower() in self.exp_apps:
  #           tmp_rst = v.lower()

  #         if v in self.rules[consts.APP_RULE][secdomain]:
  #           for app, score_count in self.rules[consts.APP_RULE][secdomain][v].iteritems():
  #             score,count = score_count['score'], score_count['support']
              
  #             if score > maxScore:
  #               predict_app = app
  #               maxScore = score
  #               occurCount = count
                
  #             elif score == maxScore and count > occurCount:
  #               predict_app = app
  #               occurCount = count
  #   predictRst = {}
  #   #predict_app, maxScore = (tmp_rst, 1) if not predict_app else (predict_app, maxScore)
  #   if predict_app:
  #     predictRst[consts.APP_RULE] = [(predict_app, maxScore)]
  #   return predictRst
