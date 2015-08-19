import consts
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
    self.appSuffix = suffix_tree(exp_apps[appType])
    self.appType = appType

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

    print ">>>[HOST] generalRules", len(generalRules)
    #############################
    # Generate specific rules
    #############################
    specificRules = defaultdict(lambda : defaultdict( lambda : defaultdict( lambda : defaultdict(lambda : {consts.SCORE:0,consts.SUPPORT:0}))))
    ruleCover = defaultdict(int)
    
    debugCounter = 0
    for tbl, pkgs in trainData.iteritems():
      for pkg in filter(lambda pkg : pkg.secdomain in generalRules, pkgs):
        for rule in filter(lambda rule : rule.key in pkg.queries, generalRules[pkg.secdomain]):
          ruleCover[rule] += 1
          for value in pkg.queries[rule.key]:
            if len(self.valueLabelCounter[value]) == 1:
                debugCounter += 1
                specificRules[pkg.host][rule.key][value][pkg.label][consts.SCORE] = rule.score
                specificRules[pkg.host][rule.key][value][pkg.label][consts.SUPPORT] += 1
    self.persist(specificRules, rule_type)
    self.__init__(self.appType)
    return self

  def _clean_db(self, rule_type):
    print consts.SQL_DELETE_KV_RULES % rule_type
    sqldao = SqlDao()
    sqldao.execute(consts.SQL_DELETE_KV_RULES % rule_type)
    sqldao.commit()
    sqldao.close()

  def load_rules(self):
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

  def classify(self, pkg):
    predictRst = {}
    for rule_type in self.rules:
      maxScore, occurCount = -1, -1
      prediction = None
      evidence = (None, None)

      for k, k_rules in self.rules[rule_type].get(pkg.host, {}).iteritems():
        for v in pkg.queries.get(k, []):          
          for label, score_count in k_rules.get(v, {}).iteritems():
            score, count = score_count[consts.SCORE], score_count[consts.SUPPORT]

            if score > maxScore or (score == maxScore and count > occurCount):
              prediction = label
              maxScore, occurCount = score, count
              evidence = (k, v)
            elif not prediction:
              pass
             #print 'value not in rules', k.encode('utf-8'), v.encode('utf-8'), pkg.app
      predictRst[rule_type] = (prediction, maxScore, evidence[0], evidence[1])

      if not predictRst[consts.APP_RULE][0]:
        for k, values in pkg.queries.iteritems():
          label = map(lambda v : self.classify_suffix_app(v), values)
          predictRst[consts.APP_RULE] = (label[0], 1, k)

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
            support = patterns[host][key][value][label][consts.SUPPORT]
            params.append((label, support, confidence, host, key, value, rule_type))
            if confidence > max_confidence:
              max_confidence = confidence
              max_support = support
              max_label = label
            elif confidence == max_confidence and support > max_support:
              max_support = support
              max_label = label
          #params.append((max_label, max_support, max_confidence, host, key+'='+value, rule_type))
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
