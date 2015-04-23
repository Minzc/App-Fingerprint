import sys
import operator
from fp_growth import find_frequent_itemsets
from utils import loadfile, rever_map, Relation
from itertools import imap
from collections import defaultdict, namedtuple


class FNode:
    def __init__(self, feature):
        self.feature = feature
        self.appCounter = defaultdict(int)

    def inc(self, app):
        self.appCounter[app] += 1

    def genRules(self):
        totalNum = sum(self.appCounter.values())
        # feature, app, support, confidence
        return [(self.feature, app, 1.0 * num, 1.0 * num / totalNum) for app, num in self.appCounter.iteritems()]


class FPRuler:
    def __init__(self):
        # feature, app, host
        self.rulesSet = []


    def addRules(self, rules):
        tmprules = {}
        for feature, app, host in rules:
            tmprules.setdefault(host, {})
            tmprules[host][feature] = app
        self.rulesSet.append(tmprules)


    def classify(self, record):
        # TODO change it to map
        labelRsts = []
        for rulesID, rules in enumerate(self.rulesSet):
            rst = set()
            features = _get_record_f(record)
            if record.host in rules:
                for rule, label in rules[record.host].iteritems():
                    if rule.issubset(features):
                        rst.add(label)
            # TODO handle multiple predicted apps
            if len(rst) > 0:
                labelRsts.append(rst.pop())
            else:
                labelRsts.append(None)
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

    for head_seg in filter(None, record.add_header.split('\n')):
        if len(head_seg) > 2:
            features.append(head_seg.replace(' ', '').strip())

    for agent_seg in filter(None, record.agent.split(' ')):
        if len(agent_seg) < 2:
          features.append(agent_seg.replace(' ', ''))
    host = record.host if record.host else record.dst
    features.append(host)
    features.append(record.app)

    return features


def _encode_data(records=None, minimum_support = 2):
    if not records:
        records = load_pkgs(limit)
    
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
      items = {k for k, v in f_counter.iteritems() 
          if v > minimum_support and len(f_company[k]) < 4}
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
        # encode_transaction = [ itemIndx[item] for item in set(transaction[:-2])
        # encode_transaction.append(itemIndx[host])
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


def _rever_map(mapObj):
    return {v: k for k, v in mapObj.items()}


def _load_data(filepath):
    records = []
    loadfile(filepath, lambda x: records.append(x.split(' ')))
    return records


def _load_appindx(filepath):
    apps = {}
    loadfile(filepath, lambda x: apps.setdefault(x.split('\t')[1], x.split('\t')[0]))
    return apps


def _load_findx(filepath):
    features = {}
    loadfile(filepath, lambda x: features.setdefault(x.split('\t')[1], x.split('\t')[0]))
    return features


def _load_records_hst(filepath):
    recordHost = []
    loadfile(filepath, lambda x: recordHost.append(x))
    return recordHost


def _gen_rules(transactions, tSupport, tConfidence, featureIndx):
    fNodes = {}
    ###########################
    # Apriori Version
    ###########################
    # apriori = Apriori(transactions)
    # fNodes = apriori.apriori(tSuppoert)

    ###########################
    # Single Item Version
    ###########################
    """
    rules = []
    for transaction in transactions:
        fts, app = transaction[:-1], transaction[-1]
        for ft in fts:
            if ft not in fNodes:
               fNodes[ft] = FNode(ft)
            fNodes[ft].inc(app)
    for fNode in fNodes.values():
        for rule in fNode.genRules():
            feature, app, support, confidence = rule
            if support > tSupport and confidence > tConfidence:
                rules.append(({feature},  confidence, support, app))
    """

    ###########################
    # FP-tree Version
    ###########################
    rules = []
    for itemset, support, tag_dist in find_frequent_itemsets(transactions, tSupport, True):
        max_clss = max(tag_dist.iteritems(), key=operator.itemgetter(1))[0]
        if tag_dist[max_clss] * 1.0 / support > tConfidence:
            confidence = max(tag_dist.values()) * 1.0 / support
            rules.append((frozenset(itemset), confidence, support, max_clss))
    
    print ">>>Finish Rule Generating"
    return rules


def _prune_rules(t_rules, records):
    # Sort generated rules according to its confidence, support and length
    t_rules.sort(key=lambda v: (v[1], v[2], len(v[0])), reverse=True)
    cover_num = [0] * len(records)
    for rule, _, _, classlabel in t_rules:
        for r_id, record in enumerate( records):
            if rule.issubset(record) and cover_num[r_id] < 4:
                cover_num[r_id] += 1
                yield (rule, classlabel, r_id)



def mine_fp(records, tSuppoert, tConfidence):
    ################################################
    # Mine App Features
    ################################################

    encodedRecords, appIndx, featureIndx, recordHost = _encode_data(records)
    # (feature, app, host index)
    rules = _gen_rules(encodedRecords, tSuppoert, tConfidence, _rever_map(featureIndx))
    # feature, app, host
    # rules : set()
    rules = _prune_rules(rules, encodedRecords)
    # change encoded features back to string
    decodedRules = set()
    for rule in rules:
        rule_str = {featureIndx[itemcode] for itemcode in rule[0]}
        rule_str = frozenset(rule_str)
        decodedRules.add((rule_str, appIndx[rule[1]], recordHost[rule[2]]))

    classifier = FPRuler()
    classifier.addRules(decodedRules)
    ################################################
    # Mine Company Features
    ################################################
    for record in records:
        if record.company:
            record.app = record.company
    encodedRecords, appIndx, featureIndx, recordHost = _encode_data(records)
    # (feature, app, host index)
    rules = _gen_rules(encodedRecords, tSuppoert, tConfidence, _rever_map(featureIndx))
    # feature, app, host
    # rules : set()
    rules = _prune_rules(rules, encodedRecords)
    # change encoded features back to string
    decodedRules = set()
    for rule in rules:
        rule_str = {featureIndx[itemcode] for itemcode in rule[0]}
        rule_str = frozenset(rule_str)
        decodedRules.add((rule_str, appIndx[rule[1]], recordHost[rule[2]]))
    classifier.addRules(decodedRules)
    
    return classifier


def mining_fp_local(filepath, tSuppoert, tConfidence):
    records = _load_data(filepath)
    appIndx = _load_appindx('app_index.txt')
    featureIndx = _load_findx('featureIndx.txt')
    recordHost = _load_records_hst('records_host.txt')

    # (feature, app, host index)
    rules = _gen_rules(records, tSuppoert, tConfidence)

    # feature, app, host
    rules = _prune_rules(rules, records)

    coverage = 0
    totalApp = set()
    for indx, record in enumerate(records):
        totalApp.add(record[-1])
        for feature in record[:-1]:
            if feature in rules:
                coverage += 1
                break
    coveredApp = set()
    for rule in rules:
        coveredApp.add(rule[1])
        rule = (featureIndx[rule[0]], appIndx[rule[1]], recordHost[rule[2]])

    print 'total:', len(records), 'coverage:', coverage, 'totalApp:', len(totalApp), 'coverApp:', len(coveredApp)


def revers():
    appIndx = load_appindx('app_index.txt')
    featureIndx = load_findx('featureIndx.txt')


class Apriori:
    def __init__(self, records):
        # encodedRecords: ({Features}, app)
        self.records = records

    def _scanD(self, candidates, tSupport):
        fNodes = {}
        for record in self.records:
            features, app = record
            for can in candidates:
                if can.issubset(features):
                    fNodes.setdefault(can, FNode(can))
                    fNodes[can].inc(app)

        retlist = {}
        support_data = {}
        for freqSet, fNode in fNodes.items():
            if fNode.appCounter.max() >= tSupport:
                retlist[freqSet] = fNode
        return retlist

    def _genC1(self):
        C1 = []
        for record in self.records:
            for i in record[0]:
                C1.append([i])
        return map(frozenset, C1)

    def _aprioriGen(self, freqSets, k):
        retList = []
        lenLk = len(freqSets)
        for i in range(lenLk):
            for j in range(i + 1, lenLk):
                combinedSet = freqSets[i] | freqSets[j]
                if len(combinedSet) == k:
                    retList.append(combinedSet)
        return retList

    def apriori(self, minSupport):
        C1 = self._genC1()
        # {set(): FNode}
        L1 = self._scanD(C1, minSupport)
        L = [L1]

        k = 2
        while (len(L[k - 2]) > 0 and k < 2):
            Ck = self._aprioriGen(L[k - 2].keys(), k)
            Lk = self._scanD(Ck, minSupport)
            L.append(Lk)
            k += 1
        rst = {}
        for i in L:
            rst.update(i)
        return rst


if __name__ == '__main__':
    if sys.argv[1] == 'mine':
        mining_fp_local(sys.argv[2], tSuppoert=int(sys.argv[3]), tConfidence=float(sys.argv[4]))
	

