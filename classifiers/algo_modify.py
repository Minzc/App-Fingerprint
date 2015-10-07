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
    self.featureAppTbl = defaultdict(lambda : defaultdict( lambda : defaultdict( lambda : defaultdict(set))))
    self.featureCompanyTbl = defaultdict(lambda : defaultdict( lambda : defaultdict( lambda : defaultdict(set))))
    self.valueAppCounter = defaultdict(set)
    self.valueCompanyCounter = defaultdict(set)
    self.rules = {}
    exp_apps = load_exp_app()
    self.appRegex = {}
    for appName in exp_apps[appType]:
      if '.' in appName:
        self.appRegex[appName] = re.compile(appName)
      else:
        self.appRegex[appName] = re.compile('='+appName)

    self.appType = appType

  
  def _prune_general_rules(self, generalRules, trainData):
    '''
    Input
    - generalRules : {secdomain : [(secdomain, key, score, labelNum), rule, rule]}
    - trainData : { tbl : [ packet, packet, ... ] }
    '''
    ruleCoverage = defaultdict(lambda : defaultdict(set))
    ruleScores = {}
    ruleLabelNum = {}
    
    for tbl, pkgs in trainData.iteritems():
      for pkg in filter(lambda pkg : pkg.secdomain in generalRules, pkgs):
        for rule in filter(lambda rule : rule.key in pkg.queries, generalRules[pkg.secdomain]):
          for value in pkg.queries[rule.key]:
            value = value.strip()
            ruleCoverage[pkg.host][rule.key].add(tbl + '#' + str(pkg.id))
            ruleScores[ (pkg.host, rule.key) ] = rule.score
            ruleLabelNum[ (pkg.host, rule.key) ] = rule.labelNum

    PKG_IDS= 1
    prunedGenRules = defaultdict(list)
    for host, keyNcoveredIds in ruleCoverage.iteritems():
      keyNcoveredIds = sorted(keyNcoveredIds.items(), key=lambda keyNid : len(keyNid[PKG_IDS]))
      for i in range(len(keyNcoveredIds)):
        ifKeepRule = (True, None)
        iKey, iCoveredIds = keyNcoveredIds[i]
        for j in range(i+1, len(keyNcoveredIds)):
          jKey, jCoveredIds = keyNcoveredIds[j]
          if iCoveredIds.issubset(jCoveredIds) and ruleScores[ (host, iKey) ] < ruleScores[ (host, jKey) ]:
            ifKeepRule = (False, jKey)
        if ifKeepRule[0]:
          rule = consts.Rule(host, ruleI[0], ruleScores[ (host, iKey) ],  ruleLabelNum[ (host, iKey) ])
          prunedGenRules[host].append(rule)
          # print 'Keep', host, ruleI[0], ruleScores[ (host, ruleI[0]) ]
        # else:
        #   print 'Pruned'
        #   print host, ruleI[0], ruleScores[host][ruleI[0]], 'pruned by:', ifKeepRule[1]
        #   print '-' * 10

      # print '='*10
    return prunedGenRules


  def _count(self, featureTbl, valueLabelCounter):
    '''
    Give score to very ( secdomain, key ) pairs
    Input
    - featureTbl : 
        Relationships between host, key, value and label(app or company) from training data
        { secdomain : { key : { label : {value} } } }
    - valueLabelCounter : 
        Relationships between labels(app or company)
        { app : {label} }
    '''
    # secdomain -> app -> key -> value -> tbls
    # secdomain -> key -> (label, score)
    keyScore = defaultdict(lambda : defaultdict(lambda : {consts.LABEL:set(), consts.SCORE:0 }))
    for secdomain in featureTbl:
      for k in featureTbl[secdomain]:
        cleanedK = k.replace("\t", "")
        for label in featureTbl[secdomain][k]:
          for v, tbls in featureTbl[secdomain][k][label].iteritems():
            if len(valueLabelCounter[v]) == 1:
              keyScore[secdomain][cleanedK][consts.SCORE] += \
                    (len(tbls) - 1) / float(len(featureTbl[secdomain][k][label]) * len(featureTbl[secdomain][k]))
              keyScore[secdomain][cleanedK][consts.LABEL].add(label)
    return keyScore

  def _generate_keys(self, keyScore):
    '''
    Find interesting ( secdomain, key ) pairs
    Input
    - keyScore : scores for ( secdomain, key ) pairs
    Output
    - generalRules : 
        Rule = ( secdomain, key, score, labelNum ) defined in consts/consts.py
        {secdomain : [Rule, Rule, Rule, ... ]}
    '''
    Rule = consts.Rule
    generalRules = defaultdict(list)
    for secdomain in keyScore:
      for key in keyScore[secdomain]:
        labelNum = len(keyScore[secdomain][key][consts.LABEL]) 
        score = keyScore[secdomain][key][consts.SCORE]
        if labelNum == 1 or score <= 0.5:
          continue
        generalRules[secdomain].append(Rule(secdomain, key, score, labelNum))
    for secdomain in generalRules:
      generalRules[secdomain] = sorted(generalRules[secdomain], key=lambda rule: rule.score, reverse = True)
    return generalRules

  def _generate_rules(self, trainData, generalRules, valueLabelCounter):
    '''
    Generate specific rules
    Input
    - trainData : { tbl : [ packet, packet, packet, ... ] }
    - generalRules :
        Generated in _generate_keys()
        {secdomain : [Rule, Rule, Rule, ... ]}
    - valueLabelCounter : Relationships between value and labels
    Output
    - specificRules : specific rules for apps
        { host : { key : { value : { label : { rule.score, support : { tbl, tbl, tbl } } } } } }
    '''
    specificRules = defaultdict(lambda : defaultdict( lambda : defaultdict( lambda : defaultdict(lambda : {consts.SCORE:0,consts.SUPPORT:set()}))))
    
    for tbl, pkgs in trainData.iteritems():
      for pkg in filter(lambda pkg : pkg.host in generalRules, pkgs):
        for rule in filter(lambda rule : rule.key in pkg.queries, generalRules[pkg.host]):
          for value in pkg.queries[rule.key]:
            value = value.strip()
            if len(valueLabelCounter[value]) == 1 and len(value) != 1:
                specificRules[pkg.host][rule.key][value][pkg.label][consts.SCORE] = rule.score
                specificRules[pkg.host][rule.key][value][pkg.label][consts.SUPPORT].add(tbl)
    return specificRules

  def train(self, trainData, rule_type):
    for tbl in trainData.keys():
      for pkg in trainData[tbl]:
        for k,v in pkg.queries.items():
          if pkg.secdomain == 'bluecorner.es' or pkg.host == 'bluecorner.es' or pkg.label == 'com.bluecorner.totalgym':
            #print 'OK contains bluecorner', pkg.secdomain
            pass
          map(lambda x : self.featureAppTbl[pkg.secdomain][k][pkg.label][x].add(tbl), v)
          map(lambda x : self.featureCompanyTbl[pkg.secdomain][k][pkg.company][x].add(tbl), v)
          map(lambda x : self.valueAppCounter[x].add(pkg.label), v)
          map(lambda x : self.valueCompanyCounter[x].add(pkg.company), v)
    ##################
    # Count
    ##################
    appKeyScore = self._count(self.featureAppTbl, self.valueAppCounter)
    companyKeyScore = self._count(self.featureCompanyTbl, self.valueCompanyCounter)
    #############################
    # Generate interesting keys
    #############################
    appGeneralRules = self._generate_keys(appKeyScore)
    companyGeneralRules = self._generate_keys(companyKeyScore)
    #############################
    # Pruning general rules
    #############################
    appGeneralRules = self._prune_general_rules(appGeneralRules, trainData)
    companyGeneralRules = self._prune_general_rules(companyGeneralRules, trainData)
    print ">>>[KV] appGeneralRules", len(appGeneralRules)
    print ">>>[KV] companyGeneralRules", len(companyGeneralRules)
    #############################
    # Generate specific rules
    #############################
    appSpecificRules = self._generate_rules(trainData, appGeneralRules, self.valueAppCounter)
    companySpecificRules = self._generate_rules(trainData, companyGeneralRules, self.valueCompanyCounter)

    #############################
    # Persist rules
    #############################
    self.persist(appSpecificRules, rule_type)
    self.__init__(self.appType)
    return self

  def _clean_db(self, rule_type):
    print '>>> [KVRULES]', consts.SQL_DELETE_KV_RULES % rule_type
    sqldao = SqlDao()
    sqldao.execute(consts.SQL_DELETE_KV_RULES % rule_type)
    sqldao.commit()
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




