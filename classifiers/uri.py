from collections import defaultdict
import const.consts as consts

from classifiers.classifier import AbsClassifer
from const.app_info import AppInfos
from const.dataset import DataSetIter as DataSetIter
from sqldao import SqlDao
from utils import load_exp_app, feature_lib
import re


class Node:
    def __init__(self, string, parent):
        self.__string = string
        self.appInfos = set()
        self.counter = defaultdict(lambda: defaultdict(int))
        self.children = {}
        self.__parent = parent

    def inc(self, appInfo, tbl):
        if appInfo not in self.appInfos:
            self.appInfos.add(appInfo)
        self.counter[appInfo.package][tbl] += 1

    def add(self, childNode):
        self.children[childNode.feature] = childNode

    @property
    def feature(self):
        return self.__string

    def get(self, feature):
        return self.children[feature] if feature in self.children else None

    @property
    def parent(self):
        return self.__parent

    def tblInfo(self, appInfo):
        return self.counter[appInfo]

    @property
    def categories(self):
        return {appInfo.category for appInfo in self.appInfos}

    @property
    def companies(self):
        return {appInfo.company for appInfo in self.appInfos}


class AppMiner:
    @staticmethod
    def filter(node): return len(node.appInfos) == 1
    @staticmethod
    def label(pkg): return pkg.appInfo.package
    @staticmethod
    def features(fLib, appInfos):
        appInfo = list(appInfos)[0]
        constrain = set(appInfo.package.split('.')) | set(appInfo.website.split('.'))
        return fLib[consts.APP_RULE][appInfo.package] & constrain

class CompanyMiner:
    @staticmethod
    def filter(node): return len(node.companies) == 1
    @staticmethod
    def label(pkg): return pkg.appInfo.company
    @staticmethod
    def features(fLib, appInfos):
        appInfos = list(appInfos)
        features = fLib[consts.COMPANY_RULE][appInfos[0].package]
        for appInfo in appInfos[1:]:
            features &= fLib[consts.COMPANY_RULE][appInfo.package]
        return features



def part(fs, target):
    for featureSet in fs:
        featureSet = list(featureSet) if type(featureSet) == tuple else [featureSet]
        matchFeature = filter(lambda x: x in target, featureSet)
        ifValid = len(matchFeature) == len(featureSet)
        if ifValid:
            return True
    return False


def whole(f, host): return len(f & set(host.split('.')))


def get_f(pkg):
    features = [pkg.refer_host, pkg.host]
    features += filter(None, map(lambda x: x.strip(), pkg.path.split('/')))
    return features


class UriClassifier(AbsClassifer):
    def __init__(self, appType):
        self.root = Node(None, None)
        expApp = {label: AppInfos.get(appType, label) for label in load_exp_app()[appType]}
        self.fLib = feature_lib(expApp)
        self.pathLabel = defaultdict(set)
        self.hostLabel = defaultdict(set)
        self.rules = {consts.APP_RULE: defaultdict(lambda: defaultdict()),
                      consts.COMPANY_RULE: defaultdict(lambda: defaultdict()),
                      consts.CATEGORY_RULE: defaultdict(lambda: defaultdict())}

    def add(self, node, features, appInfo, tbl):
        if len(features) == 0:
            return

        child = node.get(features[0])
        child = child if child is not None else Node(features[0], node)
        node.add(child)
        child.inc(appInfo, tbl)
        self.add(child, features[1:], appInfo, tbl)

    def __count(self, features, label):
        map(lambda host: self.hostLabel[host].add(label), features[0:2])
        map(lambda pathSeg: self.pathLabel[pathSeg].add(label), features[2:])

    def __host_rules(self, trainSet):
        def __count(miner,check,ruleType):
            hostNodes = self.root.children.values()
            tmpR = defaultdict(set)
            for node in filter(miner.filter, hostNodes):
                features = miner.features(self.fLib, node.appInfos)
                if check(features, node.feature):
                    tmpR[ruleType].add(node.feature)

            for tbl, pkg in DataSetIter.iter_pkg(trainSet):
                if pkg.host in tmpR[ruleType]:
                    hostRules[ruleType][(pkg.rawHost, None, miner.label(pkg))].add(tbl)

        hostRules = defaultdict(lambda: defaultdict(set))
        __count(AppMiner, whole, consts.APP_RULE)
        __count(CompanyMiner, part, consts.COMPANY_RULE)
        return hostRules

    def __path_rules(self, trainSet):
        pathRules = defaultdict(lambda: defaultdict(set))
        tmpR = defaultdict(set)
        for pathSeg, labels in filter(lambda x: len(x[1]) == 1, self.pathLabel.iteritems()):
            label = list(labels)[0]
            fs = self.fLib[consts.APP_RULE][label]
            ifValid = part(fs, pathSeg)
            if ifValid:
                tmpR[consts.APP_RULE].add(pathSeg)

        for tbl, pkg in DataSetIter.iter_pkg(trainSet):
            pkgFs = set(get_f(pkg)[2:])
            for pathSeg in tmpR[consts.APP_RULE]:
                if pathSeg in pkgFs:
                    pathRules[consts.APP_RULE][(pkg.rawHost, pathSeg, pkg.label)].add(tbl)

        return pathRules

    def __f_valid(self, feature, package, ruleType):
        for featureSet in self.fLib[ruleType][package]:
            featureSet = list(featureSet) if type(featureSet) == tuple else [featureSet]
            matchFeature = filter(lambda x: x in feature, featureSet)
            ifValid = len(matchFeature) == len(featureSet)
            if ifValid:
                return True
        return False

    def train(self, trainData, rule_type):
        rawHost = defaultdict(set)
        for tbl, pkg in DataSetIter.iter_pkg(trainData):
            rawHost[pkg.host].add(pkg.rawHost)
            features = get_f(pkg)
            self.__count(features, pkg.app)
            self.add(self.root, features[1:], pkg.appInfo, tbl)

        hostRules = self.__host_rules(trainData)
        pathRules = self.__path_rules(trainData)

        self._persist(hostRules)
        self._persist(pathRules)

    def load_rules(self):
        QUERY = 'SELECT label, pattens, host, rule_type, support FROM patterns where agent IS  NULL and paramkey IS NULL'
        sqldao = SqlDao()
        counter = 0
        for label, pathSeg, host, ruleType, support in sqldao.execute(QUERY):
            counter += 1
            pathSegObj = re.compile(pathSeg, re.IGNORECASE) if pathSeg is not None else ''
            self.rules[ruleType][host][pathSegObj] = (label, support)
        print '>>> [URI Rules#loadRules] total number of rules is', counter, 'Type of Rules', len(self.rules)
        sqldao.close()

    def c(self, package):
        """
        Return {type:[(label, confidence)]}
        :param package:
        """
        labelRsts = {}
        for rule_type, rules in self.rules.iteritems():
            rst = consts.NULLPrediction
            if package.rawHost in rules:
                if '' in rules[package.rawHost]:
                    if package.refer_rawHost == '':
                        label = rules[package.rawHost][''][0]
                        rst = consts.Prediction(label, 1.0, ('Host', package.rawHost))
                else:
                    print '[URI] 205'
                    for pathSegObj in rules[package.rawHost]:
                        if pathSegObj.search(package.origPath):
                            label = rules[package.rawHost][pathSegObj][0]
                            rst = consts.Prediction(label, 1, ("Path", package.rawHost, pathSegObj.pattern))
            labelRsts[rule_type] = rst
        return labelRsts

    @staticmethod
    def _persist(rules):
        sqldao = SqlDao()
        QUERY = consts.SQL_INSERT_CMAR_RULES
        params = []
        for ruleType in rules:
            for rule, tbls in rules[ruleType].items():
                host, pathSeg, label = rule
                if pathSeg is not None:
                    if not pathSeg[-1].isalnum():
                        pathSeg = pathSeg[:-1]
                    if not pathSeg[0].isalnum():
                        pathSeg = pathSeg[1:]
                    pathSeg = '\b' + re.escape(pathSeg) + '\b'
                params.append((label, pathSeg, 1, len(tbls), host, ruleType))
            print "Total Number of Rules is", len(rules[ruleType])
        sqldao.executeBatch(QUERY, params)
        sqldao.close()



# def __homo_rules(self, hostRules, trainSet, tmpR):
#         print '======'
#
#         def __hst_valid(self, host, package, ruleType):
#             features = self.fLib[consts.APP_RULE][package]
#             commons = features & set(host.split('.'))
#             return len(commons) > 0
#
#         def iter_branch(hostNode):
#             stack = [hostNode]
#             while len(stack) > 0:
#                 n, stack = stack[0], stack[1:]
#                 for node in n.children.values():
#                     stack.insert(0, node)
#                 if len(n.appInfos) == 1 and len(n.children) > 0:
#                     appInfo = list(n.appInfos)[0]
#                     pathSegValid = self.__f_valid(n.feature, appInfo.package, consts.CATEGORY_RULE)
#                     hostValid = self.__hst_valid(hostNode.feature, appInfo.package, consts.CATEGORY_RULE)
#
#                     if pathSegValid and hostValid and n.feature not in tmpR[consts.APP_RULE]:
#                         print '[FEATURE]', n.feature.encode(
#                             'utf-8'), '[HOST]', hostNode.feature, '[APP]', appInfo.package
#                         yield (hostNode.feature, n.feature)
#
#         hostNodes = filter(lambda node: node.feature not in hostRules[consts.APP_RULE], self.root.children.values())
#         tmpR = defaultdict(lambda: defaultdict(set))
#         for hNode in hostNodes:
#             for host, pathSeg in iter_branch(hNode):
#                 tmpR[consts.APP_RULE][host].add(pathSeg)
#
#         rules = defaultdict(lambda: defaultdict(set))
#         for tbl, pkg in DataSetIter.iter_pkg(trainSet):
#             if pkg.host in tmpR[consts.APP_RULE]:
#                 pkgFs = set(get_f(pkg)[2:])
#                 for pathSeg in tmpR[consts.APP_RULE][pkg.host]:
#                     if pathSeg in pkgFs:
#                         rules[consts.APP_RULE][(pkg.rawHost, pathSeg, pkg.label)].add(tbl)
#         return rules