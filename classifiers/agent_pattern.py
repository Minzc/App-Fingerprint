from __future__ import absolute_import, division, print_function, unicode_literals
import re
from collections import defaultdict
import utils
from classifiers.classifier import AbsClassifer
from const.dataset import DataSetIter, DataSetFactory
from const import consts, conf
from utils import flatten, const
from sqldao import SqlDao
import math



def persist(appRule):
    """
    Input
    :param appRule : {regex: {app1, app2}}
    """
    sqldao = SqlDao()
    params = set()

    for rules in appRule.values():
        for rule in rules:
            prefix, suffix, signature, fuse_score, current_len, c, support = rule
            prefix = ' '.join(prefix)
            suffix = ' '.join(suffix)
            signature = ' '.join(signature)
            params.add((None, prefix, signature, suffix, c, support, fuse_score, 3, consts.APP_RULE))

    sqldao.executeBatch(const.sql.SQL_INSERT_AGENT_RULES, params)
    sqldao.close()


SPLITTER = re.compile("[" + r'''"#$%&*+,:<=>?@[\]^`{|}~ \-''' + "]")


class RulesTree:
    def __init__(self):
        class Node:
            def __init__(self, item, clss, score, id):
                self.i, self.clss, self.score, self.child, self.id = item, clss, score, {}, id

            def add(self, item, clss, score, id):
                self.child[item] = Node(item, clss, score, id)

        self.root = Node(None, None, 0, -1)

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




class AgentBoundary(AbsClassifer):
    def cal_sup(self, mdb):
        host_app = defaultdict(set)
        for (i, _, _) in mdb:
            host_app[self.id_host_map[self.db[i]]].add(self.db[i][1])
        support = sum([len(apps) / len(self.host_app_map[host]) for host, apps in host_app.items()])
        return support / len(host_app)


    def __init__(self):
        self.rules = {}
        self.support_t = conf.agent_support
        self.conf_t = conf.agent_score
        self.K = conf.agent_K
        self.db = []
        print('Support', self.support_t, 'Score', self.conf_t)

    def train(self, trainSet, ruleType, ifPersist=True):
        counter = defaultdict(set); self.id_host_map = {}; self.host_app_map = defaultdict(set)
        totalApps = set()
        for tbl, pkg in DataSetIter.iter_pkg(trainSet):
            totalApps.add(pkg.app)
            map(lambda w: counter[w].add(pkg.app), filter(None, SPLITTER.split(utils.process_agent(pkg.agent))))
            if pkg.agent == 'None':
                continue
            segAgent = tuple(['^'] + filter(None, SPLITTER.split(utils.process_agent(pkg.agent))) + ['$'])
            if len(segAgent) > 0:
                self.db.append((segAgent, pkg.app))
                self.id_host_map[(segAgent, pkg.app)] = pkg.secdomain
                self.host_app_map[pkg.secdomain].add(pkg.app)

        self.support_t = conf.agent_support * len(totalApps)

        self.db = list(set(self.db))
        print("Data Size", len(self.db))
        #self.db = [(tuple(['^'] + filter(None, SPLITTER.split(utils.process_agent(x[0]))) + ['$']), x[1]) for x in self.db]

        for (t, c) in self.db:
            map(lambda w: counter[w].add(c), t)
        self.IDF = utils.cal_idf(counter)
        self.mine_rec([], [(i, len(self.db[i][0]) - 1) for i in range(len(self.db))], 10000, False)

        if ifPersist:
            persist(self.rules)

    def mine_rec(self, prefix, mdb, gap, expSuffix):
        occurs = defaultdict(list)
        for (i, startpos) in mdb:
            seq = self.db[i][0]
            for j in xrange(startpos, -1, -1):
                if startpos - j <= gap:
                    l = occurs[seq[j]]
                    l.append((i, j - 1))
        if expSuffix:
            suffix_mdb = [
                # dbindex, last position of prefix, start position of suffix
                (i, startpos + 1, startpos + 3) for (i, startpos) in mdb if startpos + 3 < len(self.db[i][0])
                ]
            self.mine_suffix(prefix, [], suffix_mdb, 10000)

        for c, newmdb in occurs.items():
            childSupport = len({self.db[i][1] for (i, _) in newmdb})
            if childSupport > self.support_t:
                self.mine_rec([c] + prefix, newmdb, 0, True)

    def mine_suffix(self, prefix, suffix, mdb, gap):
        def idf(sig):
            return sum([self.IDF[i] for i in sig]) / len(sig)

        def rel(sig):
            import math
            # if sig == tuple(['com.mm.ilady.ipad.client']):
            #     print('prefix:', prefix, 'suffix:', suffix)
            #     print("sig:", sig, "rel:", math.sqrt(1 / len(app_sig_map[list(sig_app_map[sig])[0]])))
            #     print(list(sig_app_map[sig])[0])
            #     print(app_sig_map[list(sig_app_map[sig])[0]])
            return math.sqrt(1 / len(app_sig_map[list(sig_app_map[sig])[0]]))

        occurs = defaultdict(list)
        sig_app_map = defaultdict(set)
        app_sig_map = defaultdict(set)
        tmpRule = set()
        for (i, prefix_pos, startpos) in mdb:
            seq, app = self.db[i]
            signature = seq[prefix_pos + 1: startpos - len(suffix)]
            if len(suffix) > 0:
                tmpRule.add((i, signature))
                sig_app_map[signature].add(app)
                app_sig_map[app].add(signature)

            for j in xrange(startpos, len(seq)):
                if j - startpos <= gap:
                    l = occurs[seq[j]]
                    l.append((i, prefix_pos, j + 1))

        find_good = False
        if len(suffix) != 0:
            quality = sum([math.sqrt(idf(signature) * rel(signature)) for signature in sig_app_map]) / len(sig_app_map)
            support = self.cal_sup(mdb)
            context_score = 2 * support * quality / (support + quality)
            if context_score >= self.conf_t:
                for i, signature in tmpRule:
                    if len(sig_app_map[signature]) == 1:
                        quality = idf(signature) * rel(signature)
                        if '/' in set(signature):
                            continue
                        find_good = True
                        fuse_score = quality * context_score
                        current_len = len(prefix) + len(suffix) + len(signature)
                        if i not in self.rules:
                            self.rules[i] = list()
                        self.rules[i].append((prefix, suffix, signature, fuse_score, current_len, self.db[i][1], support))
                        self.rules[i] = sorted(self.rules[i], key=lambda x: (x[3], 10000 - x[4]), reverse=True)[:self.K]
        else:
            find_good = True

        maxSuffixSupport = 0
        if find_good:
            for (c, newmdb) in occurs.iteritems():
                childSupport = len({self.db[i][1] for (i, _, _) in newmdb})
                maxSuffixSupport = max(childSupport, maxSuffixSupport)
                if childSupport > self.support_t:
                    self.mine_suffix(prefix, suffix + [c], newmdb, 0)
        return maxSuffixSupport

    def load_rules(self):
        self.rules = {
            consts.APP_RULE: RulesTree(),
            consts.COMPANY_RULE: RulesTree(),
            consts.CATEGORY_RULE: RulesTree()
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
            length += len(re.escape(x).replace(re.escape("VERSION"), r'\b[a-z0-9-.]+\b'))
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


if __name__ == '__main__':
    tbls = ['ca_ios_packages_2015_12_10', 'ca_ios_packages_2015_05_29', 'ca_ios_packages_2016_02_22']
    # tbls = ['ios_packages_2015_08_10']
    a = AgentBoundary()
    if True:
        trainSet = DataSetFactory.get_traindata(tbls=tbls, appType=consts.IOS)
        a.train(trainSet, consts.IOS)
    a.load_rules()
    testSet = DataSetFactory.get_traindata(tbls=tbls, appType=consts.IOS)
    print("Start Testing")
    batchPredicts, groundTruth = a.classify(testSet)
    correctApp = set()
    wrongApp = set()
    for pID, predict in batchPredicts.items():
        if predict[consts.APP_RULE].label is None:
            continue
        if predict[consts.APP_RULE].label == groundTruth[pID]:
            correctApp.add(groundTruth[pID])
        else:
            wrongApp.add(groundTruth[pID])

    print("Correct APP", len(correctApp - wrongApp))
    print("Wrong APP", len(wrongApp))
