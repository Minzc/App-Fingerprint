import consts
from sqldao import SqlDao
from utils import load_pkgs, loadfile, load_appinfo, loadExpApp
from collections import defaultdict, namedtuple
import operator
from classifier import AbsClassifer


DEBUG = False

class KVClassifier(AbsClassifer):
  def __init__(self):
    self.featureTbl = defaultdict(lambda : defaultdict( lambda : defaultdict( lambda : defaultdict(set))))
    self.valueAppCounter = defaultdict(set)
    self.valueCompanyCounter = defaultdict(set)
    self.appCompany = loadExpApp()

  def train(self, training_data=None):
    appDict = set()
    training_data = _load_train_data() if not training_data else training_data
    
    for tbl in training_data.keys():
      for pkg in training_data[tbl]:
        appDict.add(pkg.app)
        for k,v in pkg.queries.items():
          if pkg.secdomain == 'bluecorner.es' or pkg.host == 'bluecorner.es' or pkg.app == 'com.bluecorner.totalgym':
            #print 'OK contains bluecorner', pkg.secdomain
            pass
          map(lambda x : self.featureTbl[pkg.secdomain][pkg.app][k][x].add(tbl), v)
          map(lambda x : self.valueAppCounter[x].add(pkg.app), v)
          map(lambda x : self.valueCompanyCounter[x].add(pkg.company), v)
    
    ##################
    # Count
    ##################
    # secdomain -> app -> key -> value -> tbls
    # secdomain -> key -> (app, score)
    keyScore = defaultdict(lambda : defaultdict(lambda : {'app':set(), 'score':0}))
    for secdomain in self.featureTbl:
      for app in self.featureTbl[secdomain]:
        for k in self.featureTbl[secdomain][app]:
          for v, tbls in self.featureTbl[secdomain][app][k].iteritems():
            if secdomain == 'bluecorner.es':
              pass
            if len(self.valueAppCounter[v]) == 1:
              cleaned_k = k.replace("\t", "")
              keyScore[secdomain][cleaned_k]['score'] += (len(tbls) - 1) / float(len(self.featureTbl[secdomain][app][k]))
              keyScore[secdomain][cleaned_k]['app'].add(app)
    #############################
    # Generate interesting keys
    #############################
    Rule = namedtuple('Rule', 'secdomain, key, score, appNum')
    general_rules = defaultdict(list)
    for secdomain in keyScore:
      for key in keyScore[secdomain]:
        appNum = len(keyScore[secdomain][key]['app']) 
        score = keyScore[secdomain][key]['score']
        if appNum == 1 or score == 0:
          continue
        general_rules[secdomain].append(Rule(secdomain, key, score, appNum))
    for secdomain in general_rules:
      general_rules[secdomain] = sorted(general_rules[secdomain], key=lambda rule: rule.score, reverse = True)

    print "general_rules", len(general_rules)
    #############################
    # Generate specific rules
    #############################
    specific_rules = defaultdict(lambda : defaultdict( lambda : defaultdict( lambda : defaultdict(lambda : {'score':0, 'support':0}))))
    ruleCover = defaultdict(int)
    
    debug_counter = 0
    for tbl, pkgs in training_data.iteritems():
      for pkg in filter(lambda pkg : pkg.secdomain in general_rules, pkgs):
        for rule in filter(lambda rule : rule.key in pkg.queries, general_rules[pkg.secdomain]):
          ruleCover[rule] += 1
          for value in pkg.queries[rule.key]:
            if len(self.valueAppCounter[value]) == 1:
                debug_counter += 1
                specific_rules[pkg.host][rule.key][value][pkg.app]['score'] = rule.score
                specific_rules[pkg.host][rule.key][value][pkg.app]['support'] += 1
    self.persist(specific_rules)

    return self

  def _clean_db(self):
    sqldao = SqlDao()
    sqldao.execute('DELETE FROM patterns WHERE paramkey IS NOT NULL and pattens IS NULL')
    sqldao.commit()
    sqldao.close()


  def load_rules_without_key(self):
    print '>>> [KVClassifier] load rules'
    sqldao = SqlDao()
    self.rules[consts.APP_RULE] = defaultdict(lambda : defaultdict( lambda : defaultdict( lambda : defaultdict(lambda : {'score':0, 'support':0}))))
    self.rules[consts.COMPANY_RULE] = defaultdict(lambda : defaultdict( lambda : defaultdict( lambda : defaultdict(lambda : {'score':0, 'support':0}))))
    QUERY = 'SELECT paramkey, paramvalue, host, label, confidence, rule_type, support FROM patterns WHERE paramkey IS NOT NULL'
    counter = 0
    for key, value, host, label, confidence, rule_type, support in sqldao.execute(QUERY):
      counter += 1
      self.rules[rule_type][''][value][label]['score'] = confidence
      self.rules[rule_type][''][value][label]['support'] = support
    print counter

  def load_rules(self):
    self.rules = {}
    sqldao = SqlDao()
    self.rules[consts.APP_RULE] = defaultdict(lambda : defaultdict( lambda : defaultdict( lambda : defaultdict(lambda : {'score':0, 'support':0}))))
    self.rules[consts.COMPANY_RULE] = defaultdict(lambda : defaultdict( lambda : defaultdict( lambda : defaultdict(lambda : {'score':0, 'support':0}))))
    QUERY = 'SELECT paramkey, paramvalue, host, label, confidence, rule_type, support FROM patterns WHERE paramkey IS NOT NULL'
    counter = 0
    for key, value, host, label, confidence, rule_type, support in sqldao.execute(QUERY):
      counter += 1
      # key, value = kv.split('=', 1)
      host = ''
      self.rules[rule_type][host][key][value][label]['score'] = confidence
      self.rules[rule_type][host][key][value][label]['support'] = support
    print '>>> [KV Rules#Load Rules] total number of rules is', counter
    sqldao.close()

  def classify(self, pkg):
    max_score = -1
    occur_count = -1
    predict_app = None
    tmp_rst = None
    secdomain = ''
    if secdomain in self.rules[consts.APP_RULE]:
      for k in self.rules[consts.APP_RULE][secdomain]:
        if k in pkg.queries:
          for v in pkg.queries[k]:
            if v.lower() in self.appCompany:
              tmp_rst = v.lower()

            if v in self.rules[consts.APP_RULE][secdomain][k]:
              for app, score_count in self.rules[consts.APP_RULE][secdomain][k][v].iteritems():
                score,count = score_count['score'], score_count['support']
                
                if score > max_score:
                  predict_app = app
                  max_score = score
                  occur_count = count
                  
                elif score == max_score and count > occur_count:
                  predict_app = app
                  occur_count = count
    predict_rst = {}
    predict_app, max_score = (tmp_rst, 1) if not predict_app else (predict_app, max_score)
    if predict_app:
      predict_rst[consts.APP_RULE] = [(predict_app, max_score)]
    return predict_rst  

  def classify_without_key(self, pkg):
    max_score = -1
    occur_count = -1
    predict_app = None
    tmp_rst = None
    secdomain = ''
    if secdomain in self.rules[consts.APP_RULE]:
      for k in pkg.queries:
        for v in pkg.queries[k]:
          if v.lower() in self.appCompany:
            tmp_rst = v.lower()

          if v in self.rules[consts.APP_RULE][secdomain]:
            for app, score_count in self.rules[consts.APP_RULE][secdomain][v].iteritems():
              score,count = score_count['score'], score_count['support']
              
              if score > max_score:
                predict_app = app
                max_score = score
                occur_count = count
                
              elif score == max_score and count > occur_count:
                predict_app = app
                occur_count = count
    predict_rst = {}
    predict_app, max_score = (tmp_rst, 1) if not predict_app else (predict_app, max_score)
    if predict_app:
      predict_rst[consts.APP_RULE] = [(predict_app, max_score)]
    return predict_rst

  def persist(self, patterns):
    self._clean_db()
    QUERY = 'INSERT INTO patterns (label, support, confidence, host, paramkey, paramvalue, rule_type) VALUES (%s, %s, %s, %s, %s, %s, %s)'
    sqldao = SqlDao()
    # Param rules
    params = []
    for host in patterns:
      for key in patterns[host]:
        for value in patterns[host][key]:
          max_confidence = -1
          max_support = -1
          max_app = None
          for app in patterns[host][key][value]:
            confidence = patterns[host][key][value][app]['score']
            support = patterns[host][key][value][app]['support']
            params.append((app, support, confidence, host, key, value, consts.APP_RULE))
            if confidence > max_confidence:
              max_confidence = confidence
              max_support = support
              max_app = app
            elif confidence == max_confidence and support > max_support:
              max_support = support
              max_app = app
          #params.append((max_app, max_support, max_confidence, host, key+'='+value, consts.APP_RULE))
    sqldao.executeBatch(QUERY, params)
    sqldao.close()
    print ">>> [KVRules] Total Number of Rules is %s Rule type is %s" % (len(params), consts.APP_RULE)


import sys

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print 'Number of args is wrong'
    elif sys.argv[1] == 'test':
      KVPredictor()
    elif sys.argv[1] == 'train':
      KVMiner()
