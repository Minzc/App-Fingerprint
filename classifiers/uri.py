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


class UriClassifier(AbsClassifer):
    def __init__(self, appType):
        self.root = Node(None, None)
        expApp = {label: AppInfos.get(appType, label) for label in load_exp_app()[appType]}
        self.fLib = feature_lib(expApp)
        self.pathLabel = defaultdict(set)
        self.hostLabel = defaultdict(set)
        self.rules = {}
        self.rules[consts.APP_RULE] = defaultdict(lambda: defaultdict())
        self.rules[consts.COMPANY_RULE] = defaultdict(lambda: defaultdict())
        self.rules[consts.CATEGORY_RULE] = defaultdict(lambda: defaultdict())

    def add(self, node, features, appInfo, tbl):
        if len(features) == 0:
            return

        child = node.get(features[0])
        child = child if child is not None else Node(features[0], node)
        node.add(child)
        child.inc(appInfo, tbl)
        self.add(child, features[1:], appInfo, tbl)

    def __get_f(self, pkg):
        features = [pkg.refer_host, pkg.host]
        features += filter(None, map(lambda x: x.strip(), pkg.path.split('/')))
        return features

    def __count(self, features, label):
        map(lambda host: self.hostLabel[host].add(label), features[0:2])
        map(lambda pathSeg: self.pathLabel[pathSeg].add(label), features[2:])

    def __host_rules(self, trainSet):
        hostNodes = self.root.children.values()
        tmpR = defaultdict(set)
        for node in filter(lambda x: len(x.appInfos) == 1, hostNodes):
            appInfo = list(node.appInfos)[0]

            constrain = set(appInfo.package.split('.')) | set(appInfo.website.split('.'))
            features = self.fLib[consts.APP_RULE][appInfo.package] & constrain

            commons = features & set(node.feature.split('.'))
            if len(commons) > 0:
                #print node.feature, '[FEATURES]',features
                tmpR[consts.APP_RULE].add(node.feature)

        hostRules = defaultdict(lambda: defaultdict(set))
        for tbl, pkg in DataSetIter.iter_pkg(trainSet):
            if pkg.host in tmpR[consts.APP_RULE]:
                hostRules[consts.APP_RULE][(pkg.rawHost, None, pkg.label)].add(tbl)

        return hostRules

    def __path_rules(self, trainSet):
        tmpR = defaultdict(set)
        for pathSeg, labels in filter(lambda x: len(x[1]) == 1, self.pathLabel.iteritems()):
            label = list(labels)[0]
            ifValid = self.__f_valid(pathSeg, label, consts.APP_RULE)
            if ifValid: tmpR[consts.APP_RULE].add(pathSeg)

        pathRules = defaultdict(lambda: defaultdict(set))
        for tbl, pkg in DataSetIter.iter_pkg(trainSet):
            pkgFs = set(self.__get_f(pkg)[2:])
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
                print featureSet,'[ORIGIN]', matchFeature, '[Feature]', feature.encode('utf-8'), '[APP]', package
                return True
        return False

    def __hst_valid(self, host, package, ruleType):
        features = self.fLib[consts.APP_RULE][package]
        commons = features & set(host.split('.'))
        return len(commons) > 0

    def __homo_rules(self, hostRules, trainSet):
        print '======'
        def iter_branch(hostNode):
            stack = [hostNode]
            while len(stack) > 0:
                n, stack = stack[0], stack[1:]
                for node in n.children.values():
                    stack.insert(0, node)
                if len(n.appInfos) == 1 and len(n.children) > 0:
                    appInfo = list(n.appInfos)[0]
                    pathSegValid = self.__f_valid(n.feature, appInfo.package, consts.CATEGORY_RULE)
                    hostValid = self.__hst_valid(hostNode.feature, appInfo.package, consts.CATEGORY_RULE)

                    if pathSegValid and hostValid:
                        print '[FEATURE]', n.feature.encode('utf-8'),'[HOST]', hostNode.feature, '[APP]', appInfo.package
                        yield (hostNode.feature, n.feature)


        hostNodes = filter(lambda node: node.feature not in hostRules[consts.APP_RULE], self.root.children.values())
        tmpR = defaultdict(lambda: defaultdict(set))
        for hNode in hostNodes:
            for host, pathSeg in iter_branch(hNode):
                tmpR[consts.APP_RULE][host].add(pathSeg)

        rules = defaultdict(lambda: defaultdict(set))
        for tbl, pkg in DataSetIter.iter_pkg(trainSet):
            if pkg.host in tmpR[consts.APP_RULE]:
                pkgFs = set(self.__get_f(pkg)[2:])
                for pathSeg in tmpR[consts.APP_RULE][pkg.host]:
                    if pathSeg in pkgFs:
                        rules[consts.APP_RULE][(pkg.rawHost, pathSeg, pkg.label)].add(tbl)
        return rules

    def train(self, trainData, rule_type):
        rawHost = defaultdict(set)
        for tbl, pkg in DataSetIter.iter_pkg(trainData):
            rawHost[pkg.host].add(pkg.rawHost)
            features = self.__get_f(pkg)
            self.__count(features, pkg.app)
            self.add(self.root, features[1:], pkg.appInfo, tbl)

        hostRules = self.__host_rules(trainData)
        #pathRules = self.__path_rules(trainData)
        homoRules = self.__homo_rules(hostRules, trainData)

        #self._persist(hostRules)
        #self._persist(pathRules)
        self._persist(homoRules)

    def load_rules(self):
        QUERY = 'SELECT label, pattens, host, rule_type, support FROM patterns where agent IS  NULL and paramkey IS NULL'
        sqldao = SqlDao()
        counter = 0
        for label, pathSeg, host, ruleType, support in sqldao.execute(QUERY):
            counter += 1
            if pathSeg is None: pathSeg = ''
            self.rules[ruleType][host][pathSeg] = (label, support)
        print '>>> [URI Rules#loadRules] total number of rules is', counter, 'Type of Rules', len(self.rules)
        sqldao.close()

    def c(self, package):
        """
        Return {type:[(label, confidence)]}
        """
        labelRsts = {}
        pathSegs = set(self.__get_f(package)[2:])
        for rule_type, rules in self.rules.iteritems():
            rst = consts.NULLPrediction
            if package.rawHost in rules:
                if '' in rules[package.rawHost] and package.refer_rawHost == '':
                    label = rules[package.rawHost][''][0]
                    rst = consts.Prediction(label, 1.0, ('Host', package.rawHost))
                else:
                    for pathSeg in rules[package.rawHost]:
                        if pathSeg in pathSegs:
                            label = rules[package.rawHost][pathSeg][0]
                            rst = consts.Prediction(label, 1, ("Path", package.rawHost, pathSeg))
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
                params.append((label, pathSeg, 1, len(tbls), host, ruleType))
            print "Total Number of Rules is", len(rules[ruleType])
        sqldao.executeBatch(QUERY, params)
        sqldao.close()
