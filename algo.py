import consts
from sqldao import SqlDao
from utils import load_pkgs, loadfile, load_appinfo, loadExpApp, suffix_tree
from collections import defaultdict, namedtuple
import operator
from classifier import AbsClassifer


DEBUG = False

class KVClassifier(AbsClassifer):
  def __init__(self):
    self.featureTbl = defaultdict(lambda : defaultdict( lambda : defaultdict( lambda : defaultdict(set))))
    self.value_label_c = defaultdict(set)
    self.rules = {}
    exp_apps = loadExpApp()
    self.app_suffix = suffix_tree(exp_apps)

  def train(self, training_data, rule_type):
    label_dict = set()
    
    for tbl in training_data.keys():
      for pkg in training_data[tbl]:
        label_dict.add(pkg.label)
        for k,v in pkg.queries.items():
          if pkg.secdomain == 'bluecorner.es' or pkg.host == 'bluecorner.es' or pkg.label == 'com.bluecorner.totalgym':
            #print 'OK contains bluecorner', pkg.secdomain
            pass
          map(lambda x : self.featureTbl[pkg.secdomain][pkg.label][k][x].add(tbl), v)
          map(lambda x : self.value_label_c[x].add(pkg.label), v)
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
            if len(self.value_label_c[v]) == 1:
              cleaned_k = k.replace("\t", "")
              keyScore[secdomain][cleaned_k][consts.SCORE] += \
                (len(tbls) - 1) / float(len(self.featureTbl[secdomain][label][k]))
              keyScore[secdomain][cleaned_k][consts.LABEL].add(label)
    #############################
    # Generate interesting keys
    #############################
    Rule = namedtuple('Rule', 'secdomain, key, score, labelNum')
    general_rules = defaultdict(list)
    for secdomain in keyScore:
      for key in keyScore[secdomain]:
        labelNum = len(keyScore[secdomain][key][consts.LABEL]) 
        score = keyScore[secdomain][key][consts.SCORE]
        if labelNum == 1 or score == 0:
          continue
        general_rules[secdomain].append(Rule(secdomain, key, score, labelNum))
    for secdomain in general_rules:
      general_rules[secdomain] = sorted(general_rules[secdomain], key=lambda rule: rule.score, reverse = True)

    print "general_rules", len(general_rules)
    #############################
    # Generate specific rules
    #############################
    specific_rules = defaultdict(lambda : defaultdict( lambda : defaultdict( lambda : defaultdict(lambda : {consts.SCORE:0,consts.SUPPORT:0}))))
    ruleCover = defaultdict(int)
    
    debug_counter = 0
    for tbl, pkgs in training_data.iteritems():
      for pkg in filter(lambda pkg : pkg.secdomain in general_rules, pkgs):
        for rule in filter(lambda rule : rule.key in pkg.queries, general_rules[pkg.secdomain]):
          ruleCover[rule] += 1
          for value in pkg.queries[rule.key]:
            if value == 'ca-app-pub-9456426941744194':
              print self.value_label_c[value]
            if len(self.value_label_c[value]) == 1:
                debug_counter += 1
                specific_rules[pkg.host][rule.key][value][pkg.label][consts.SCORE] = rule.score
                specific_rules[pkg.host][rule.key][value][pkg.label][consts.SUPPORT] += 1
    self.persist(specific_rules, rule_type)
    self.__init__()
    return self

  def _clean_db(self, rule_type):
    print 'DELETE FROM patterns WHERE paramkey IS NOT NULL and pattens IS NULL and rule_type=%s' % rule_type
    sqldao = SqlDao()
    sqldao.execute('DELETE FROM patterns WHERE paramkey IS NOT NULL and pattens IS NULL and rule_type=%s' % rule_type)
    sqldao.commit()
    sqldao.close()

  def load_rules(self):
    self.rules = {}
    sqldao = SqlDao()
    self.rules[consts.APP_RULE] = defaultdict(lambda : defaultdict( lambda : defaultdict( lambda : defaultdict(lambda : {'score':0, 'support':0}))))
    self.rules[consts.COMPANY_RULE] = defaultdict(lambda : defaultdict( lambda : defaultdict( lambda : defaultdict(lambda : {'score':0, 'support':0}))))
    self.rules[consts.CATEGORY_RULE] = defaultdict(lambda : defaultdict( lambda : defaultdict( lambda : defaultdict(lambda : {'score':0, 'support':0}))))
    QUERY = 'SELECT paramkey, paramvalue, host, label, confidence, rule_type, support FROM patterns WHERE paramkey IS NOT NULL'
    counter = 0
    for key, value, host, label, confidence, rule_type, support in sqldao.execute(QUERY):
      counter += 1
      self.rules[rule_type][host][key][value][label][consts.SCORE] = confidence
      self.rules[rule_type][host][key][value][label][consts.SUPPORT] = support
    print '>>> [KV Rules#Load Rules] total number of rules is', counter
    sqldao.close()

  def classify(self, pkg):
    predict_rst = {}
    for rule_type in self.rules:
      max_score, occur_count = -1, -1
      prediction = None
      evidence = (None, None)

      for k, k_rules in self.rules[rule_type].get(pkg.host, {}).iteritems():
        for v in pkg.queries.get(k, []):          
          for label, score_count in k_rules.get(v, {}).iteritems():
            score, count = score_count[consts.SCORE], score_count[consts.SUPPORT]

            if score > max_score or (score == max_score and count > occur_count):
              prediction = label
              max_score, occur_count = score, count
              evidence = (k, v)
            elif not prediction:
              pass
             #print 'value not in rules', k.encode('utf-8'), v.encode('utf-8'), pkg.app
      predict_rst[rule_type] = (prediction, max_score, evidence[0], evidence[1])

      if not predict_rst[consts.APP_RULE]:
        for k, values in pkg.queries.iteritems():
          label = map(lambda v : self.classify_suffix_app(v), values)
          print label
          predict_rst[consts.APP_RULE] = (label, 1, k)

    return predict_rst

  def classify_suffix_app(self, value):
    value = value.split('.')
    node = self.app_suffix
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
    QUERY = 'INSERT INTO patterns (label, support, confidence, host, paramkey, paramvalue, rule_type) VALUES (%s, %s, %s, %s, %s, %s, %s)'
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
  #   max_score = -1
  #   occur_count = -1
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
              
  #             if score > max_score:
  #               predict_app = app
  #               max_score = score
  #               occur_count = count
                
  #             elif score == max_score and count > occur_count:
  #               predict_app = app
  #               occur_count = count
  #   predict_rst = {}
  #   #predict_app, max_score = (tmp_rst, 1) if not predict_app else (predict_app, max_score)
  #   if predict_app:
  #     predict_rst[consts.APP_RULE] = [(predict_app, max_score)]
  #   return predict_rst
