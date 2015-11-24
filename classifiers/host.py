from utils import url_clean, load_exp_app, flatten
from sqldao import SqlDao
from collections import defaultdict
import const.consts as consts
from const.app_info import AppInfos
from classifier import AbsClassifer
import re
from const.dataset import DataSetIter as DataSetIter

test_str = {'stats.3sidedcube.com', 'redcross.com'}


class HostApp(AbsClassifer):
    def __init__(self, appType):
        self.appType = appType
        self.urlLabel = defaultdict(set)
        self.substrCompany = defaultdict(set)
        self.labelAppInfo = {}
        self.rules = defaultdict(dict)
        self.fLib = defaultdict(lambda : defaultdict(set))

    @staticmethod
    def persist(patterns):
        sqldao = SqlDao()
        QUERY = consts.SQL_INSERT_HOST_RULES
        params = []
        for ruleType in patterns:
            for url, label, support in flatten(patterns[ruleType]):
                params.append((label, len(support), 1, url, ruleType))
        sqldao.executeBatch(QUERY, params)
        sqldao.close()

    def count(self, pkg):
        host = url_clean(pkg.host)
        refer_host = pkg.refer_host
        if not host:
            return

        self.labelAppInfo[pkg.label] = [pkg.website]
        map(lambda url: self.urlLabel[url].add(pkg.label), [host, refer_host])

    @staticmethod
    def _clean_db(rule_type):
        QUERY = consts.SQL_DELETE_HOST_RULES
        sqldao = SqlDao()
        sqldao.execute(QUERY % rule_type)
        sqldao.close()

    def _feature_lib(self, expApp):
        segCompany = defaultdict(set)
        segApp = defaultdict(set)
        tmpLib = defaultdict(set)
        for label, appInfo in expApp.iteritems():
            appSegs = appInfo.package.split('.')
            companySegs = appInfo.company.split(' ')
            categorySegs = appInfo.category.split(' ')
            websiteSegs = url_clean(appInfo.website).split('.')
            nameSegs = appInfo.name.split(' ')
            wholeSegs = [appSegs, companySegs, categorySegs, websiteSegs, nameSegs]
            for segs in wholeSegs:
                for seg in segs:
                    tmpLib[label].add(seg)
                    segCompany[seg].add(appInfo.company)
                    segApp[seg].add(appInfo.package)
        for label, segs in tmpLib.items():
            self.fLib[consts.COMPANY_RULE][label] = {seg for seg in segs if len(segCompany[seg]) == 1}
            self.fLib[consts.APP_RULE][label] = {seg for seg in segs if len(segApp[seg]) == 1}

    def _count(self, get_feature, get_label, trainData):
        rawHost = {}
        tmpRst = defaultdict(lambda: defaultdict(set))
        hostLabel = defaultdict(set)
        for tbl, pkg in DataSetIter.iter_pkg(trainData):
            rawHost[pkg.host] = pkg.rawHost
            rawHost[pkg.refer_host] = pkg.refer_rawHost

            for url in [pkg.host, pkg.refer_host]:
                features = get_feature(pkg)
                commons = features.intersection(set(url.split('.')))
                hostLabel[url].add(get_label(pkg))
                if len(commons) > 0:
                    tmpRst[url][get_label(pkg)].add(tbl)
                    if rawHost[url] == 'ui.bamstatic.com':
                        print 'ERROR', pkg.app, pkg.company, features.intersection(features)

        rules = defaultdict(lambda: defaultdict(set))

        for host in filter(lambda h: len(hostLabel[h]) == 1 and h in tmpRst, tmpRst):
            rules[rawHost[host]] = tmpRst[host]
        return rules

    def _count_company(self, trainData):
        get_feature = lambda pkg: self.fLib[consts.COMPANY_RULE][pkg.app]
        get_label = lambda pkg: pkg.company
        rules = self._count(get_feature, get_label, trainData)
        return rules

    def _count_app(self, trainData):
        def get_feature(pkg):
            defaultF = self.fLib[consts.APP_RULE][pkg.app]
            constrainF = defaultF.intersection((set(pkg.app.split('.')) | set(pkg.website.split('.'))))
            return constrainF

        get_label = lambda pkg: pkg.app
        rules = self._count(get_feature, get_label, trainData)
        return rules

    def train(self, trainData, rule_type):
        expApp = load_exp_app()[self.appType]
        expApp = {label: AppInfos.get(self.appType, label) for label in expApp}
        self._feature_lib(expApp)
        ########################
        # Generate Rules
        ########################
        appRule = self._count_app(trainData)
        companyRule = self._count_company(trainData)

        self.rules[consts.APP_RULE] = appRule
        self.rules[consts.COMPANY_RULE] = companyRule

        print 'number of rule', len(self.rules[consts.APP_RULE])

        self.persist(self.rules)
        self.__init__(self.appType)
        return self

    def load_rules(self):
        self.rules = {consts.APP_RULE: {}, consts.COMPANY_RULE: {}, consts.CATEGORY_RULE: {}}
        QUERY = consts.SQL_SELECT_HOST_RULES
        sqldao = SqlDao()
        counter = 0
        for host, label, ruleType, support in sqldao.execute(QUERY):
            counter += 1
            regexObj = re.compile(r'\b' + re.escape(host) + r'\b')
            self.rules[ruleType][host] = (label, support, regexObj)
        print '>>> [Host Rules#loadRules] total number of rules is', counter, 'Type of Rules', len(self.rules)
        sqldao.close()

    def count_support(self, trainData):
        LABEL = 0
        TBLSUPPORT = 1
        for tbl, pkg in DataSetIter.iter_pkg(trainData):
            for ruleType in self.rules:
                host = url_clean(pkg.host)
                refer_host = pkg.refer_host
                for url in [host, refer_host]:
                    if url in self.rules[ruleType]:
                        label = self.rules[ruleType][url][LABEL]
                        if label == pkg.label:
                            self.rules[ruleType][url][TBLSUPPORT].add(tbl)

    def _recount(self, records):
        for tbl, pkgs in records.items():
            for pkg in pkgs:
                for url, labels in self.urlLabel.iteritems():
                    if len(labels) == 1 and (url in pkg.host or url in pkg.refer_host):
                        self.urlLabel[url].add(pkg.label)

    def classify(self, testSet):
        batchPredicts = {}
        compress = defaultdict(set)
        for tbl, pkg in DataSetIter.iter_pkg(testSet):
            compress[pkg.rawHost].add(pkg)

        for rawHost, pkgs in compress.items():
            predictRst = self.c(rawHost)
            for pkg in pkgs:
                if pkg.refer_rawHost == '':
                    batchPredicts[pkg.id] = predictRst
                else:
                    batchPredicts[pkg.id] = {consts.APP_RULE:consts.NULLPrediction,
                                             consts.COMPANY_RULE:consts.NULLPrediction,
                                             consts.CATEGORY_RULE:consts.NULLPrediction}

        for tbl, pkg in DataSetIter.iter_pkg(testSet):
            predict = batchPredicts[pkg.id][consts.APP_RULE]
            if predict.label and predict.label != pkg.app:
                print predict.evidence, predict.label, pkg.app
                print '=' * 10
        return batchPredicts

    def c(self, host):
        """
        Input
        - self.rules : {ruleType: {host : (label, support, regexObj)}}
        """
        rst = {}
        for ruleType in self.rules:
            predict = consts.NULLPrediction
            for regexStr, ruleTuple in self.rules[ruleType].iteritems():
                label, support, regexObj = ruleTuple
                match = regexObj.search(host)
                if match and predict.score < support:
                    if match.start() == 0:
                        predict = consts.Prediction(label, support, (host, regexStr, support))

            rst[ruleType] = predict
        return rst
