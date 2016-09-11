from __future__ import absolute_import, division, print_function, unicode_literals
import re
from collections import defaultdict
import utils
from classifiers.classifier import AbsClassifer
from const.dataset import DataSetIter, DataSetFactory
from const import consts, conf
from utils import flatten, const
import math
import datetime

from sqldao import SqlDao


def print(*args, **kwargs):
    return __builtins__.print(*tuple(['[%s]' % str(datetime.datetime.now())] + list(args)), **kwargs)

SPLITTER = re.compile("[" + r'''"#$%&*+,:<=>?@[\]^`{|}~ \-''' + "]")

class ContextsTree:
    def __init__(self):
        class Node:
            def __init__(self, item, clss, score, id):
                self.i, self.clss, self.score, self.child = item, clss, score, {}
                self.id = id

            def add(self, item, clss, score, id):
                self.child[item] = Node(item, clss, score, id)

        self.root = Node(None, None, 0, None)

    def add_rule(self, rule):
        n = self.root
        r, clss, score, id = rule
        for i, w in enumerate(r):
            if w not in n.child:
                if i == len(r) - 1:
                    n.add(w, clss, score, id)
                else:
                    n.add(w, None, 0, None)
            n = n.child[w]

    def search(self, ws):
        rst = consts.NULLPrediction
        for i in range(len(ws)):
            n = self.root
            for j, w in enumerate(ws[i:]):
                if w in n.child:
                    n = n.child[w]
                    if n.score > rst[1]:
                        rst = consts.Prediction(n.clss, n.score, ws[i:i + j + 1], n.id)
                else:
                    break
        return rst

def context_quality(s, e):
    return 2 * s * e / (s + e)

Rules = defaultdict(list)
INFO = {}

class AgentBoundary(AbsClassifer):
    def __init__(self):
        self.rules = {}
        self.support_t = conf.agent_support
        self.conf_t = conf.agent_score
        self.K = conf.agent_K
        self.HDB = []
        print('Support', self.support_t, 'Score', self.conf_t, 'K', self.K)

    def train(self, trainSet, ruleType, ifPersist=True, datasize=0):
        counter = defaultdict(set); totalApps = set()
        for tbl, pkg in DataSetIter.iter_pkg(trainSet):
            if pkg.agent == 'None':
                continue
            map(lambda w: counter[w].add(pkg.app), filter(None, SPLITTER.split(utils.process_agent(pkg.agent))))
            segAgent = tuple(['^'] + filter(None, SPLITTER.split(utils.process_agent(pkg.agent))) + ['$'])
            self.HDB.append((segAgent, pkg.app, len(segAgent)))
            totalApps.add(pkg.app)

        self.omega = len(totalApps) * self.support_t
        self.totalApp = len(totalApps) * 1.0

        self.HDB = list(set(self.HDB))
        print("Data Size", len(self.HDB))

        for (t, c, l) in self.HDB:
            map(lambda w: counter[w].add(c), t)
        self.IDF = utils.cal_idf(counter)
        self.mine_context()
        persist(Rules)



    def mine_context(self):
        print('[%s] Start Mining Context' % str(datetime.datetime.now()))
        occurs = defaultdict(list)
        support = defaultdict(set)
        for i in range(len(self.HDB)):
            seq, app, length = self.HDB[i]
            for j in xrange(length):
                occurs[seq[j]].append((i, j))
                support[seq[j]].add(app)

        for item, apps in support.items():
            if len(apps)  > self.omega:
                self.mine_head([item], occurs[item])

    def mine_head(self, prefix, mdb):
        occurs = defaultdict(list)
        support = defaultdict(set)
        for (i, startpos) in mdb:
            if startpos + 1 < self.HDB[i][2]:
                e = self.HDB[i][0][startpos + 1]
                occurs[e].append((i, startpos + 1))
                support[e].add(self.HDB[i][1])
        self.mine_tail(mdb, prefix)
        for e, newmdb in occurs.items():
            if len(support[e])  > self.omega:
                self.mine_head(prefix + [e], newmdb)

    def mine_tail(self, mdb, prefix):
        occurs = defaultdict(list)
        support = defaultdict(set)
        for (i, startpos) in mdb:
            if startpos + 2 < len(self.HDB[i][0]):
                for j in xrange(startpos + 2, self.HDB[i][2]):
                    e = self.HDB[i][0][j]
                    occurs[e].append((i, startpos, j))
                    support[e].add(self.HDB[i][1])
        for item, apps in support.items():
            if len(apps)  > self.omega:
                self.mine_tail_rec([item], occurs[item], prefix)

    def mine_tail_rec(self, tail, mdb, head):
        occurs = defaultdict(list)
        itemSupport = defaultdict(set); support = set()
        SC = defaultdict(set); appSigMap = defaultdict(set); seqAppMap = defaultdict(set)
        for (i, hEnd, startpos) in mdb:
            support.add(self.HDB[i][1])
            seqStr = ' '.join(self.HDB[i][0][hEnd + 1 : startpos - len(tail) + 1])
            # print('Find a sequence', seqStr, 'HEAD:', head, 'TAIL:', tail, 'Origin:', self.HDB[i][0])
            SC[seqStr].add(i); seqAppMap[seqStr].add(self.HDB[i][1]); appSigMap[self.HDB[i][1]].add(seqStr)
            if startpos + 1 < self.HDB[i][2]:
                e = self.HDB[i][0][startpos + 1]
                occurs[e].append((i, hEnd, startpos + 1))
                itemSupport[e].add(self.HDB[i][1])

        print('Find a Context', head, tail)
        effective = 0
        seqQuality = {}

        for seq, apps in seqAppMap.items():
            if len(apps) == 1:
                if seq not in seqQuality:
                    inf = self.idf(seq)
                    seqQuality[seq] = inf * self.rel(seq, appSigMap, seqAppMap)
                effective += seqQuality[seq]


        contextQuality = context_quality(len(support) / self.totalApp, effective * 1.0 / len(seqAppMap))
        print('Context Quality:', contextQuality)
        if contextQuality > self.conf_t:
            for seqStr in SC.keys():
                if len(seqAppMap[seqStr]) == 1:
                    for i in SC[seqStr]:
                        if '/' in seqStr:
                            continue
                        sigQuality = seqQuality[seqStr]
                        currentLen = len(head) + len(tail) + len(seqStr.split(' '))
                        #Rules[i].append((head, tail, seqStr, contextQuality, sigQuality, currentLen, self.HDB[i][1]))
                        Rules[i].append((head, tail, seqStr, sigQuality, contextQuality, currentLen, self.HDB[i][1]))
                        Rules[i] = sorted(Rules[i], key=lambda x: (x[3], x[4], 10000 - x[5]), reverse=True)[:self.K]

        for e, newmdb in occurs.items():
            if len(itemSupport[e])  > self.omega:
                self.mine_tail_rec(tail + [e], newmdb, head)


    def idf(self, signature):
        if signature not in INFO:
            sigSeg = signature.split(' ')
            INFO[signature] = sum([self.IDF[i] for i in sigSeg]) / len(sigSeg)
        return INFO[signature]

    def rel(self, signature, appSigMap, sigAppMap):
        return math.sqrt(1 / len(appSigMap[list(sigAppMap[signature])[0]]))

    def load_rules(self):
        self.rules = {
            consts.APP_RULE: ContextsTree(),
            consts.COMPANY_RULE: ContextsTree(),
            consts.CATEGORY_RULE: ContextsTree()
        }
        self.rulesHost = {
            consts.APP_RULE: defaultdict(dict),
            consts.COMPANY_RULE: defaultdict(dict),
            consts.CATEGORY_RULE: defaultdict(dict)
        }

        sqldao = SqlDao()
        counter = 0
        length = 0
        for id, host, prefix, signature, suffix, c, support, fuse_score, ruleType, labelType in sqldao.execute(
                const.sql.SQL_SELECT_AGENT_RULES):
            x = prefix + ' ' + signature + ' ' + suffix
            if '/' in signature:
                continue
            length += len(x.split(' '))
            r = filter(None, prefix.split(' ') + signature.split(' ') + suffix.split(' '))
            self.rules[consts.APP_RULE].add_rule((r, c, fuse_score, id))
            counter += 1
        print('>>> [Agent Rules#loadRules] total number of rules is', counter, 'Type of Rules', len(self.rules))
        print('>>> [Agent Rules#loadRules] average length is', length / counter)
        sqldao.close()

    def classify(self, testSet):
        compressed = defaultdict(lambda: defaultdict(set))
        for tbl, pkg in DataSetIter.iter_pkg(testSet):
            agent = tuple(['^'] + filter(None, SPLITTER.split(utils.process_agent(pkg.agent))) + ['$'])
            compressed[agent][pkg.rawHost].add(pkg)

        batchPredicts, groundTruth = {}, {}
        for agent, host, pkgs in flatten(compressed):
            assert (type(pkgs) == set, "Type of pkgs is not correct" + str(type(pkgs)))
            predict = {}
            for ruleType in self.rules:
                predict[ruleType] = self.rules[ruleType].search(agent)

            for pkg in pkgs:
                batchPredicts[pkg.id] = predict
                groundTruth[pkg.id] = pkg.app
                if predict[consts.APP_RULE].label is not None and predict[consts.APP_RULE].label != pkg.app:
                    print('>>>[AGENT CLASSIFIER ERROR] agent:', pkg.agent, 'App:', pkg.app, 'Prediction:', predict[
                        consts.APP_RULE])
        return batchPredicts  # , groundTruth

    def c(self, pkgInfo):
        pass



def persist(appRule):
    """
    Input
    :param appRule : {regex: {app1, app2}}
    """
    sqldao = SqlDao()
    params = set()

    for rules in appRule.values():
        for rule in rules:
            prefix, suffix, signature, contextQuality, sigQuality, currentLen, c  = rule
            prefix = ' '.join(prefix)
            suffix = ' '.join(suffix)
            params.add((None, prefix, signature, suffix, c, contextQuality, sigQuality, 3, consts.APP_RULE))

    sqldao.executeBatch(utils.const.sql.SQL_INSERT_AGENT_RULES, params)
    sqldao.close()

if __name__ == '__main__':
    tbls = ['ca_ios_packages_2015_12_10', 'ca_ios_packages_2015_05_29', 'ca_ios_packages_2016_02_22',
           'ca_ios_packages_2015_12_10', 'ca_ios_packages_2015_05_29', 'ca_ios_packages_2015_05_29', 'ca_ios_packages_2015_05_29']
    for size  in [300000, 600000, 500000, 700000]:
        a = AgentBoundary()
        trainSet = DataSetFactory.get_traindata(tbls=tbls, appType=consts.IOS)
        a.train(trainSet, consts.IOS, datasize=size)
        print('Finish Rule Mining')
    # for i, rules in Rules.items():
    #     for rule in rules:
    #         print("Find a rule", rule, "Origin:", a.HDB[i][0])