import sys
from classifier import AbsClassifer
import operator

from const.dataset import DataSetIter
from features.agent import AgentEncoder
from sqldao import SqlDao
from fp_growth import find_frequent_itemsets
from collections import defaultdict, namedtuple
import const.consts as consts

DEBUG_INDEX = None
DEBUG_ITEM = 'Mrd/1.2.1 (Linux; U; Android 5.0.2; google Nexus 7) com.crossfield.casinogame_bingo/20'.lower()

FinalRule = namedtuple('Rule', 'agent, path, host, label, confidence, support')
test_str = 'com.fdgentertainment.bananakong/1.8.1 (Linux; U; Android 5.0.2; en_US; razor) Apache-HttpClient/UNAVAILABLE (java 1.4)'.lower()


def _encode_data(encoders, packages=None):
    """
    Change package to transaction
    Last item is table name
    Second from last is app
    Third from last is host.
    Do not use them as feature
    """
    transactions = []

    for package in packages:
        for encoder in encoders:
            transaction = encoder.get_feature(package)
            if transaction:
                transaction.append(package.label)
                transactions.append(transaction)

    return transactions


def _gen_rules(transactions, tSupport, tConfidence):
    '''
    Generate encoded rules
    Input
    - transactions : encoded transaction
    - tSupport     : frequent pattern support
    - tConfidence  : frequent pattern confidence
    - featureIndx  : a map between number and feature string
    - appIndex     : a map between number and app
    Return : (itemsets, confidencet, support, label)
    '''
    rules = set()
    frequentPatterns = find_frequent_itemsets(transactions, tSupport, True)
    for frequent_pattern_info in frequentPatterns:
        itemset, support, tag_dist = frequent_pattern_info
        ruleStrSet = frozenset(itemset)
        labelIndex = max(tag_dist.iteritems(), key=operator.itemgetter(1))[0]
        if tag_dist[labelIndex] * 1.0 / support >= tConfidence:
            confidence = max(tag_dist.values()) * 1.0 / support
            rules.add((ruleStrSet, confidence, support, labelIndex))

    print ">>> Finish Rule Generating. Total number of rules is", len(rules)
    return rules


class CMAR(AbsClassifer):
    def __init__(self, min_cover=3, tSupport=2, tConfidence=1.0):
        # feature, app, host
        self.rules = defaultdict(lambda: defaultdict(lambda: defaultdict()))
        self.min_cover = min_cover
        self.tSupport = tSupport
        self.tConfidence = tConfidence
        self.get_feature = [AgentEncoder()]

    def pkg2features(self, package):
        features = {}
        for encoder in self.get_feature:
            tmp = encoder.get_feature(package)
            features |= set(tmp)
        return frozenset(features)

    def _persist(self, agentRules):
        '''specificRules[rule.host][ruleStrSet][label][consts.SCORE] = rule.support'''
        sqldao = SqlDao()
        QUERY = consts.SQL_INSERT_CMAR_RULES
        params = self.get_feature[0].changeRule2Para(agentRules)


        sqldao.executeBatch(QUERY, params)
        sqldao.close()
        print "Total Number of Rules is", len(params)

    def train(self, trainSet, rule_type):
        packages = []
        for tbl, pkg in DataSetIter.iter_pkg(trainSet):
            packages.append(pkg)
        print "#CMAR:", len(packages)
        encodedpackages = _encode_data(self.get_feature, packages)
        ''' Rules format : (feature, confidence, support, label) '''
        rules = _gen_rules(encodedpackages, self.tSupport, self.tConfidence)
        ''' Prune duplicated rules'''
        # rules = _remove_duplicate(rules)
        ''' feature, app, host '''
        rules = self._prune_rules(rules, trainSet, self.min_cover)
        ''' change encoded features back to string '''
        self._persist(rules)
        self.__init__()
        return self

    def load_rules(self):
        self.rules = {}
        self.rules[consts.APP_RULE] = defaultdict(lambda: defaultdict())
        self.rules[consts.COMPANY_RULE] = defaultdict(lambda: defaultdict())
        self.rules[consts.CATEGORY_RULE] = defaultdict(lambda: defaultdict())
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

    def _clean_db(self, rule_type):
        sqldao = SqlDao()
        sqldao.execute(consts.SQL_DELETE_CMAR_RULES % (rule_type))
        sqldao.commit()
        sqldao.close()

    def c(self, package):
        '''
        Return {type:[(label, confidence)]}
        '''
        labelRsts = {}
        features = self.pkg2features(package)
        for rule_type, rules in self.rules.iteritems():
            rst = consts.NULLPrediction
            max_confidence = 0
            for host in [package.host, '']:
                for rule, label_confidence in rules[host].iteritems():
                    label, confidence = label_confidence
                    if rule.issubset(features) and confidence > max_confidence:  # and confidence > max_confidence:
                        max_confidence = confidence
                        rst = consts.Prediction(label, confidence, rule)

            labelRsts[rule_type] = rst
            if rule_type == consts.APP_RULE and rst != consts.NULLPrediction and rst.label != package.app:
                print rst, package.app
                print '=' * 10
        return labelRsts

    def _prune_rules(self, tRules, trainSet, min_cover=3):
        '''
        Input t_rules: ( rules, confidence, support, class_label ), get from _gen_rules
        Input packages: list of packets
        Return
        - specificRules host -> ruleStrSet -> label -> {consts.SUPPORT, consts.SCORE}
        defaultdict(lambda : defaultdict(lambda : defaultdict(lambda : { consts.SCORE: 0, consts.SUPPORT: 0 })))
        '''
        import datetime
        ts = datetime.datetime.now()
        cover_num = defaultdict(int)
        tblSupport = defaultdict(set)
        packageInfos = defaultdict(set)
        for tbl, pkg in DataSetIter.iter_pkg(trainSet):
            for encoder in self.get_feature:
                featuresNhost = encoder.get_feature(pkg)
                if len(featuresNhost) > 0:
                    features = frozenset(featuresNhost)
                    packageInfos[pkg.label].add((features, tbl))
        print 'Len of Rules is', len(tRules)
        for rule, confidence, support, classlabel in tRules:
            for packageInfo in packageInfos[classlabel]:
                features, tbl = packageInfo
                if rule.issubset(features):
                    tblSupport[rule].add(tbl)

        tRules = sorted(tRules, key=lambda x: len(tblSupport[x]), reverse=True)
        agentRules = []
        for rule in tRules:
            ruleStrSet, confidence, support, classlabel = rule
            for packageInfo in packageInfos[classlabel]:
                package, tbl = packageInfo
                if cover_num[package] <= min_cover and ruleStrSet.issubset(package) and len(tblSupport[ruleStrSet]) > 1:
                    cover_num[package] += 1
                    for encoder in self.get_feature:
                        agent, host = encoder.change2Rule(ruleStrSet)
                        if host is None: host = ''
                        r = FinalRule(agent, None, host, classlabel, confidence, support)
                        agentRules.append(r)

        print ">>> Pruning time:", (datetime.datetime.now() - ts).seconds
        return agentRules


#def _remove_duplicate(rawRules):
#     '''
#   Input
#   - rawRules : [(ruleStrSet, confidence, support, label), ...]
#   '''
#     rules = defaultdict(list)
#     for rule in rawRules:
#         rules[rule[3]].append(rule)
#     prunedRules = []
#     print 'Total number of rules', len(rawRules)
#     for label in rules:
#         '''From large to small set'''
#         sortedRules = sorted(rules[label], key=lambda x: len(x[0]), reverse=True)
#         root = {}
#         for i in range(len(sortedRules)):
#             ifKeep = True
#             iStrSet = sortedRules[0]
#             for j in range(i + 1, len(sortedRules)):
#                 jStrSet = sortedRules[j][0]
#                 if jStrSet.issubset(iStrSet):
#                     ifKeep = False
#             if ifKeep:
#                 prunedRules.append(sortedRules[i])
#     return prunedRules
