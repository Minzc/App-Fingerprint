from __future__ import absolute_import, division, print_function, unicode_literals
import re
from collections import defaultdict
import utils
from const.dataset import DataSetIter, DataSetFactory
from const import consts
import math
import datetime

# This is designed to test the speed of the algorithm with rule generation on the fly

def print(*args, **kwargs):
    return __builtins__.print(*tuple(['[%s]' % str(datetime.datetime.now())] + list(args)), **kwargs)

SPLITTER = re.compile("[" + r'''"#$%&*+,:<=>?@[\]^`{|}~ \-''' + "]")

class ContextsTree:
    def __init__(self):
        class Node:
            def __init__(self, item, clss, score):
                self.i, self.clss, self.score, self.child = item, clss, score, {}

            def add(self, item, clss, score):
                self.child[item] = Node(item, clss, score)

        self.root = Node(None, None, 0)

    def add_rule(self, rule):
        n = self.root
        r, clss, score = rule
        for i, w in enumerate(r):
            if w not in n.child:
                if i == len(r) - 1:
                    n.add(w, clss, score)
                else:
                    n.add(w, None, 0)
            n = n.child[w]

    def search(self, ws):
        rst = consts.NULLPrediction
        for i in range(len(ws)):
            n = self.root
            for j, w in enumerate(ws[i:]):
                if w in n.child:
                    n = n.child[w]
                    if n.score > rst[1]:
                        rst = consts.Prediction(n.clss, n.score, ws[i:i + j + 1])
                else:
                    break
        return rst

def context_quality(s, e):
    return 2 * s * e / (s + e)

Rules = defaultdict(list)
INFO = {}

class AgentBoundary():
    def __init__(self, support):
        self.rules = {}
        self.support_t = support
        self.conf_t = 0.2
        self.K = 1
        self.HDB = []
        print('Support', self.support_t, 'Score', self.conf_t, 'K', self.K)

    def train(self, trainSet):
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


        print("Data Size", len(self.HDB))

        for (t, c, l) in self.HDB:
            map(lambda w: counter[w].add(c), t)
        self.IDF = utils.cal_idf(counter)
        self.mine_context()


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
                        sigQuality = seqQuality[seqStr]
                        currentLen = len(head) + len(tail) + len(seqStr.split(' '))
                        Rules[i].append((head, tail, seqStr, contextQuality, sigQuality, currentLen, self.HDB[i][1]))
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





if __name__ == '__main__':
    tbls = ['ios_packages_2015_08_04', 'ios_packages_2015_10_16','ios_packages_2015_10_21',
                'ca_ios_packages_2015_12_10', 'ca_ios_packages_2015_05_29', 'ca_ios_packages_2016_02_22',
                'chi_ios_packages_2015_07_20','chi_ios_packages_2015_09_24','chi_ios_packages_2015_12_15']
    trainSet = DataSetFactory.get_traindata(tbls=tbls, appType=consts.IOS)
    for support  in [0.2, 0.4, 0.6, 0.8]:
        a = AgentBoundary(support)
        a.train(trainSet)
        print('Finish Rule Mining')