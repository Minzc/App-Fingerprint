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



class FPRuler:
    def __init__(self):
        # feature, app, host
        self.rulesSet = {}


    def _addRules(self, rules, ruleType):
        tmprules = {}
        for feature, app, host, confidence, _ in rules:
            tmprules.setdefault(host, {})
            tmprules[host][feature] = (app, confidence)
        self.rulesSet[ruleType] = tmprules

    def loadRules(self):
      sqldao = SqlDao()
      SQL = "SELECT label, pattens, host, rule_type, confidence FROM patterns where pattens is not NULL"
      for label, patterns, host, rule_type, confidence in sqldao.execute(SQL):
        self.rulesSet.setdefault(rule_type, {})
        self.rulesSet[rule_type].setdefault(host, {})
        patterns = frozenset(patterns.split("$"))
        self.rulesSet[rule_type][host][patterns] = (label, confidence)


    def classify(self, record):
      '''
      Return {type:[(label, confidence)]}
      '''
      labelRsts = {}
      for rulesID, rules in self.rulesSet.iteritems():
          rst = []
          tmpapp = record.app
          record.app = ''
          features = _get_record_f(record)
          if record.host in rules.keys():
              for rule, label_confidence in rules[record.host].iteritems():
                  label, confidence = label_confidence
                  if rule.issubset(features):
                      rst.append(label_confidence)
          if len(rst) > 0:
              labelRsts[rulesID] = rst
          else:
              labelRsts[rulesID] = None
          record.app = tmpapp
      return labelRsts


def _get_record_f(record):
    """Get package features"""
    features = filter(None, record.path.split('/'))
    # queries = record.querys
    # for k, vs in filter(None, queries.items()):
    #     if len(k) < 2:
    #         continue
    #     features.append(k)
    #     for v in vs:
    #         if len(v) < 2:
    #             continue
    #         features.append(v.replace(' ', '').replace('\n', ''))

    # for head_seg in filter(lambda head_seg : len(head_seg) > 2, record.add_header.split('\n')):
    #     features.append(head_seg.replace(' ', '').strip())

    # for agent_seg in filter(None, agent_clean(record.agent).split(' ')):
    #     agent_seg = agent_seg.replace(' ', '')
    #     if len(agent_seg) > 2:
    #         features.append(agent_seg)
    if record.json : 
      features += record.json
    features.append(record.agent)
    host = record.host if record.host else record.dst
    features.append(host)
    features.append(record.app)

    return features


def _encode_data(records=None, minimum_support = 2):
    records = load_pkgs(limit) if not records else records
    
    def _get_transactions(records):
      """Change package to transaction"""
      f_counter = defaultdict(int)
      f_company = defaultdict(set)
      processed_transactions = []

      for record in records:
          transaction = _get_record_f(record)
          processed_transactions.append(transaction)
          # Last item is app
          for item in transaction[:-1]:
              f_counter[item] += 1
              f_company[item].add(record.company)

      # Get frequent 1-item
      items = {item for item, num in f_counter.iteritems() 
          if num > minimum_support and len(f_company[item]) < 4}
      return processed_transactions, items
    

    processed_transactions, items = _get_transactions(records)


    itemIndx = defaultdict(lambda: len(itemIndx))
    recordHost = []
    def _encode_transaction(transaction):
        """Change string items to numbers"""
        host = transaction[-2]
        app = transaction[-1]
        # Prune infrequent items
        # Host and app are not included in transaction now
        encode_transaction = [ itemIndx[item] for item in set(transaction[:-1])
                if item in items]

        recordHost.append(host)
        return (app, encode_transaction)

    train_data = []
    for trans_tuple in imap(_encode_transaction, processed_transactions):
        train_data.append(trans_tuple)

    # train_data
    # all features are encoded; decode dictionary is itemIndx
    # ((app, [f1, f2, f3, host]))
    # 1 is added to avoid index overlap 
    start_indx = len(itemIndx) + 1
    appIndx = defaultdict(lambda : start_indx + len(appIndx))
    encodedRecords = []


    for app, encode_transaction in train_data:
        # host = encode_transaction[-1]
        # Append class lable at the end of a transaction
        encode_transaction.append(appIndx[app])
        encodedRecords.append(encode_transaction)
    
    # encodedRecords: ([Features, app])
    return encodedRecords, rever_map(appIndx), rever_map(itemIndx), recordHost

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

def _prune_rules(t_rules, records, min_cover = 3):
    '''
    Input t_rules: ( rules, confidence, support, class_label ), get from _gen_rules
    Input records: list of packets
    Return: (rule, label, record_id, confidence, support)
    '''
    import datetime
    ts = datetime.datetime.now()

    # Sort generated rules according to its confidence, support and length
    t_rules.sort(key=lambda v: (v[1], v[2], len(v[0])), reverse=True)
    t_rules = _remove_duplicate(t_rules)

    cover_num = defaultdict(int)
    record_ids = {frozenset(record):i  for i, record in enumerate(records)}
    index_records = defaultdict(list)
    # Change records to sets
    map(lambda record: index_records[record[-1]].append(frozenset(record)), records)
    records = index_records
    
    for rule, confidence, support, classlabel in t_rules:
        for record in records[classlabel]:
            if cover_num[record] <= min_cover and rule.issubset(record):
                cover_num[record] += 1
                yield Rule(rule, classlabel, record_ids[record], confidence, support)
    print ">>> Pruning time:", (datetime.datetime.now() - ts).seconds


def _persist(rules, rule_type):
    sqldao = SqlDao()
    QUERY = 'INSERT INTO patterns (label, pattens, confidence, support, host, rule_type) VALUES (%s, %s, %s, %s, %s, %s)'
    params = []
    for rule in rules:
        params.append((rule.label, ','.join(rule.rule), rule.confidence, rule.support, rule.host, rule_type))
    sqldao.executeBatch(QUERY, params)
    sqldao.close()
    print "Total Number of Rules is", len(rules)

class CMAR:
  def __init__(self, min_cover):
    self.classifier = FPRuler()
    self.min_cover = min_cover

  def _clean_db(self):
      sqldao = SqlDao()
      sqldao.execute('DELETE FROM patterns WHERE kvpattern IS NULL')
      sqldao.commit()
      sqldao.close()

  def _train(self, records, tSupport, tConfidence, ruleType):
      print "#CMAR:", len(records)
      encodedRecords, appIndx, featureIndx, recordHost = _encode_data(records)
      # Rules format : (feature, confidence, support, label)
      rules = _gen_rules(encodedRecords, tSupport, tConfidence, rever_map(featureIndx))
      # feature, app, host
      rules = _prune_rules(rules, encodedRecords, self.min_cover)
      # change encoded features back to string
      decodedRules = set()
      tmp = set()
      for rule in rules:
          rule_str = frozenset({featureIndx[itemcode] for itemcode in rule[0]})
          charactor = {recordHost[rule.host]}
          charactor.add(rule_str)
          # if charactor not in tmp:
          tmp.add(frozenset(charactor))
          decodedRules.add(Rule(rule_str, appIndx[rule.label], recordHost[rule.host], rule.confidence, rule.support))

      self.classifier._addRules(decodedRules, ruleType)
      _persist(decodedRules, ruleType)

  def train(self, records, tSupport=2, tConfidence=0.8):
      self._clean_db()
      ################################################
      # Mine App Features
      ################################################
      self._train(records, tSupport, tConfidence, consts.APP_RULE)
      ################################################
      # Mine Company Features
      ################################################
      '''
      app_backup = {}
      for record in records:
          app_backup[record.id] = record.app
          if record.company:
              record.app = record.company
      self._train(records, tSupport, tConfidence, consts.COMPANY_RULE)
      for record in records:
        record.app = app_backup[record.id]
      '''
      return self.classifier

  def classify(self, record):
    return self.classifier.classify(record)

if __name__ == '__main__':
    if sys.argv[1] == 'mine':
        mining_fp_local(sys.argv[2], tSupport=int(sys.argv[3]), tConfidence=float(sys.argv[4]))
