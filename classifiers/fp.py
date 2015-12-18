import sys

import re

from classifier import AbsClassifer
import operator
from const.dataset import DataSetIter
from features.agent import AgentEncoder, AGENT, PATH, HOST
from sqldao import SqlDao
from fp_growth import find_frequent_itemsets
from collections import defaultdict, namedtuple
import const.consts as consts
from utils import if_version

FinalRule = namedtuple('Rule', 'agent, path, host, label, confidence, support')


class Rule:
    def __init__(self, itemLst, confidence, support, label):
        assert len(itemLst) != 0
        self.itemLst = itemLst
        self.confidence = confidence
        self.support = support
        self.label = label
        self.tblSupport = set()
        self.itemSet = frozenset(itemLst)

    def add_tbl(self, tbl):
        self.tblSupport.add(tbl)

    def export(self):
        agent = None
        pathSeg = None
        host = None
        for str in self.itemSet:
            if HOST in str:
                host = str
            if AGENT in str:
                agent = str
            if PATH in str:
                pathSeg = str
        return FinalRule(agent, pathSeg, host, self.label, self.confidence, self.support)


def prune_host(items):
    if len(items) == 1 and HOST in items[0]:
        return False
    return True

def prune_path(items):
    if len(items) == 1 and PATH in items[0]:
        return False
    return True

def sort_key(item):
    if AGENT in item:
        return 1
    elif PATH in item:
        return 2
    else:
        return 3


def change_support(compressDB, rules, support):
    import datetime
    ####################################
    # Compress database and get table support
    ####################################
    ts = datetime.datetime.now()
    print 'Len of Rules is', len(rules)
    for r in rules:
        for packageInfo in compressDB[r.label]:
            features, tbl = packageInfo
            if r.itemSet.issubset(features):
                r.add_tbl(tbl)
    tmpRules = []
    for r in rules:
        r.support = len(r.tblSupport)
        if r.support >= support:
            tmpRules.append(r)
    return tmpRules

def _gen_rules(transactions, tSupport, tConfidence):
    """
    Generate encoded rules
    Input
    - transactions : encoded transaction
    - tSupport     : frequent pattern support
    - tConfidence  : frequent pattern confidence
    - featureIndx  : a map between number and feature string
    - appIndex     : a map between number and app
    Return : (itemsets, confidencet, support, label)
    """
    rules = set()
    frequentPatterns = find_frequent_itemsets(transactions, tSupport, True)
    for frequent_pattern_info in frequentPatterns:
        itemset, support, tag_dist = frequent_pattern_info
        itemset = sorted(itemset, key=sort_key)

        labelIndex = max(tag_dist.iteritems(), key=operator.itemgetter(1))[0]
        if tag_dist[labelIndex] * 1.0 / support >= tConfidence:
            confidence = max(tag_dist.values()) * 1.0 / support
            if '[HOST]:mt0.googleapis.com' in set(itemset):
                print '[fp104]', tag_dist, confidence, tag_dist[labelIndex] * 1.0 / support
            assert confidence <= 1
            # if prune_host(itemset):
            if prune_path(itemset):
                r = Rule(itemset, confidence, support, labelIndex)
                rules.add(r)



    print ">>> Finish Rule Generating. Total number of rules is", len(rules)
    return rules


def _db_coverage(rules, compressDB, min_cover=3):
    """
    Input t_rules: ( rules, confidence, support, class_label ), get from _gen_rules
    Input packages: list of packets
    Return
    - specificRules host -> ruleStrSet -> label -> {consts.SUPPORT, consts.SCORE}
    defaultdict(lambda : defaultdict(lambda : defaultdict(lambda : { consts.SCORE: 0, consts.SUPPORT: 0 })))
    """

    ####################################
    # Prune by data base coverage
    ####################################
    def rank(rule):
        return rule.confidence, rule.support, len(rule.itemSet)

    tRules = sorted(rules, key=lambda x: rank(x), reverse=True)
    rules = set()
    coverNum = defaultdict(int)
    for rule in tRules:
        for item in rule.itemSet:
            if '[PATH]:loader2.gif' in item:
                print '[FP129]', rule.itemSet, rule.label
        for pkgNtbl in compressDB[rule.label]:
            package, tbl = pkgNtbl
            if coverNum[package] <= min_cover and rule.itemSet.issubset(package):
                coverNum[package] += 1
                r = rule.export()
                rules.add(r)
    return rules


def _remove_duplicate(rules):
    """
    Input
    - rawRules : [(ruleStrSet, confidence, support, label), ...]
    """

    def travers(node, ancestors):
        if node.label is not None:
            assert node.support != 0
            assert node.confidence != 0
            assert len(ancestors) != 0
            yield Rule(ancestors, node.confidence, node.support, node.label)
        if len(node.children) > 0:
            for item, child in node.children.items():
                for rule in travers(child, ancestors + [item]):
                    yield rule

    def rank(node, rule):
        strSet, confidence, support, label = rule.itemSet, rule.confidence, rule.support, rule.label
        if node.confidence > confidence:
            return 1
        elif node.confidence == confidence and node.support > support:
            return 1
        elif node.confidence == confidence and node.support == support:
            return 1
        return 0

    rules = sorted(rules, key=lambda r: (len(r.itemSet), sort_key(r.itemLst[0])))

    root = Node(None)
    for rule in rules:
        node = root
        strSet, confidence, support, label = rule.itemLst, rule.confidence, rule.support, rule.label
        for item in strSet:
            if '[PATH]:loader2.gif' in item:
                print '[FP170]', strSet, label
            if item in node.children:
                node = node.children[item]
                if node.label == label:
                    if rank(node, rule) == 1:
                        break
                    else:
                        node.label, node.support, node.confidence = None, 0, 0
            else:
                node.children[item] = Node(item)
                node = node.children[item]
                node.support = support
                node.confidence = confidence

        if node.label is None:
            node.label = label
        else:
            assert node.label == label

    rules = [rule for rule in travers(root, [])]

    return rules

def _clean_db(rule_type):
    sqldao = SqlDao()
    sqldao.execute(consts.SQL_DELETE_CMAR_RULES % rule_type)
    sqldao.commit()
    sqldao.close()


class CMAR(AbsClassifer):
    def __init__(self, min_cover=3, tSupport=2, tConfidence=1.0):
        # feature, app, host
        self.rules = defaultdict(lambda: defaultdict(lambda: defaultdict()))
        self.min_cover = min_cover
        self.tSupport = tSupport
        self.tConfidence = tConfidence
        self.encoder = AgentEncoder()

    def _encode_data(self, trainSet, fList):
        """
        Change package to transaction
        Last item is table name
        Second from last is app
        Third from last is host.
        Do not use them as feature
        """
        def prune_path(item):
            item = item.replace(PATH, '')
            if if_version(item) == True:
                return True
            if len(re.sub('^[0-9]+$', '', item)) == 0:
                return True
            if len(item) == 1:
                return True
            return False

        transactions = []
        for tbl, package in DataSetIter.iter_pkg(trainSet):
            agent, pathSeg, host = self.encoder.get_f(package)
            base = []
            if host is not None and 'mt0.googleapis.com' in host:
                print '[fp246] host',host, package.label, pathSeg
            if host is not None and len(fList[host]) >= self.tSupport:
                base.append(host)
            if agent is not None and len(fList[agent]) >= self.tSupport:
                base.append(agent)

            for item in pathSeg:
                if  len(fList[item]) >= self.tSupport and prune_path(item) == False:
                    transactions.append(base + [item] + [package.label])

            if len(pathSeg) == 0:
                transactions.append(base + [package.label])

        return transactions

    def _persist(self, ruleType, rules):
        """specificRules[rule.host][ruleStrSet][label][consts.SCORE] = rule.support"""
        sqldao = SqlDao()
        QUERY = consts.SQL_INSERT_CMAR_RULES
        params = self.encoder.changeRule2Para(rules, ruleType)
        sqldao.executeBatch(QUERY, params)
        sqldao.close()
        print "Total Number of Rules is", len(params)

    def train(self, trainSet, ruleType):
        self.encoder = AgentEncoder()
        compressDB = defaultdict(set)
        fList = defaultdict(set)
        for tbl, pkg in DataSetIter.iter_pkg(trainSet):
            assert (pkg.label != pkg.app, tbl, pkg.app)

            # remove request with refer
            if pkg.refer_rawHost:
                continue

            features = frozenset(self.encoder.get_f_list(pkg))
            map(lambda f: fList[f].add(tbl), features)
            compressDB[pkg.label].add((features, tbl))
        trainList = self._encode_data(trainSet, fList)
        ''' Rules format : (feature, confidence, support, label) '''
        rules = _gen_rules(trainList, self.tSupport, self.tConfidence)
        rules = change_support(compressDB, rules, self.tSupport)
        ''' Prune duplicated rules'''
        print '[CMAR] Before pruning', len(rules)
        rules = _remove_duplicate(rules)
        print '[CMAR] After pruning', len(rules)
        ''' feature, app, host '''
        rules = _db_coverage(rules, compressDB, self.min_cover)
        ''' change encoded features back to string '''
        self._persist(ruleType, rules)
        return self

    def load_rules(self):
        self.rules = {consts.APP_RULE: defaultdict(lambda: defaultdict()),
                      consts.COMPANY_RULE: defaultdict(lambda: defaultdict()),
                      consts.CATEGORY_RULE: defaultdict(lambda: defaultdict())}
        sqldao = SqlDao()
        counter = 0
        SQL = consts.SQL_SELECT_CMAR_RULES
        for label, patterns, agent, host, ruleType, support in sqldao.execute(SQL):
            counter += 1
            rule = []
            if patterns is not None:
                rule.append(patterns)
            if agent is not None:
                rule.append(agent)
            if host is not None:
                rule.append(host)
            rule = frozenset(rule)
            if host is not None:
                self.rules[ruleType][host][rule] = (label, support)
            else:
                self.rules[ruleType][''][rule] = (label, support)
        sqldao.close()
        print '>>>[CMAR] Totaly number of rules is', counter
        for ruleType in self.rules:
            print '>>>[CMAR] Rule Type %s Number of Rules %s' % (ruleType, len(self.rules[ruleType]))

    def c(self, package):
        """
        Return {type:[(label, confidence)]}
        """
        labelRsts = {}
        features = frozenset(self.encoder.get_f_list(package))
        for rule_type, rules in self.rules.iteritems():
            rst = consts.NULLPrediction
            if not package.refer_rawHost:
                max_confidence = 0
                for host in [HOST + package.host, '']:
                    for rule, label_confidence in rules[host].iteritems():
                        label, confidence = label_confidence
                        if rule.issubset(features) and confidence > max_confidence:  # and confidence > max_confidence:
                            max_confidence = confidence
                            rst = consts.Prediction(label, confidence, rule)

            labelRsts[rule_type] = rst
            if rule_type == consts.CATEGORY_RULE and rst != consts.NULLPrediction and rst.label != package.category:
                print '[WRONG]', rst, package.app, package.category
                print '=' * 10
        return labelRsts


class Node:
    def __init__(self, item):
        self.item = item
        self.label = None
        self.children = {}
        self.confidence = 0
        self.support = 0
