from utils import  url_clean, load_exp_app,flatten
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

    def persist(self, patterns):
        sqldao = SqlDao()
        QUERY = consts.SQL_INSERT_HOST_RULES
        params = []
        for ruleType in patterns:
            for url, label, support in flatten(patterns[ruleType]):
                params.append((label, len(support), 1, url, ruleType))
        sqldao.executeBatch(QUERY, params)
        sqldao.close()

    def _check(self, url, label, string):
        urlSegs = set(url.split('.'))
        strSegs = set(string.split('.'))
        commonStrs = urlSegs.intersection(strSegs)
        if label == 'net.ohmychef.startup':
            print commonStrs
        if len(commonStrs.intersection(self.fLib[label])) == 0:
            return False
        return True

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
        self.fLib = defaultdict(set)
        segCategory = defaultdict(set)
        for label, appInfo in expApp.iteritems():
            appSegs = appInfo.package.split('.')
            companySegs = appInfo.company.split(' ')
            categorySegs = appInfo.category.split(' ')
            websiteSegs = url_clean(appInfo.website).split('.')
            nameSegs = appInfo.name.split(' ')
            wholeSegs = [appSegs, companySegs, categorySegs, websiteSegs, nameSegs]
            for segs in wholeSegs:
                for seg in segs:
                    self.fLib[label].add(seg)
                    segCategory[seg].add(appInfo.category)
        for label, segs in self.fLib.items():
            self.fLib[label] = {seg for seg in segs if len(segCategory[seg]) == 1}


    def _count(self, get_feature, get_label, trainData):
        rawHost = {}
        tmpRst = defaultdict(lambda : defaultdict(set))
        hostLabel = defaultdict(set)
        for tbl, pkg in DataSetIter.iter_pkg(trainData):
            rawHost[pkg.host] = pkg.rawHost
            rawHost[pkg.refer_host] = pkg.refer_rawHost

            for url in [pkg.host, pkg.refer_host]:
                features = get_feature(pkg, url)
                hostLabel[url].add(get_label(pkg))
                if len(self.fLib[pkg.app].intersection(features)) > 0:
                    tmpRst[url][get_label(pkg)].add(tbl)
                    if get_label(pkg) == '0' or get_label(pkg) == 0:
                        print 'ERROR', pkg.app, pkg.company

        rules = defaultdict(lambda : defaultdict(set))

        for host in filter(lambda host: len(hostLabel[host]) == 1,tmpRst):
            rules[rawHost[host]] = tmpRst[host]
        return rules

    def _count_company(self, trainData):
        get_feature = lambda pkg, url : self.fLib[pkg.app]
        get_label = lambda pkg : pkg.company
        rules = self._count(get_feature, get_label, trainData)
        return rules

    def _count_app(self, trainData):
        get_feature = lambda pkg, url : set(pkg.app.split('.')) | set(pkg.website.split('.'))
        get_label = lambda pkg : pkg.app
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

        self.count_support(trainData)
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

    def c(self, pkg):
        """
        Input
        - self.rules : {ruleType: {host : (label, support, regexObj)}}
        :param pkg: http packet
        """
        rst = {}
        for ruleType in self.rules:
            predict = consts.NULLPrediction
            if pkg.refer_rawHost == '':
                for regexStr, ruleTuple in self.rules[ruleType].iteritems():
                    label, support, regexObj = ruleTuple
                    host = pkg.rawHost
                    match = regexObj.search(host)
                    if match and predict.score < support:
                        if match.start() == 0:
                            predict = consts.Prediction(label, support, (host, regexStr, support))


            rst[ruleType] = predict
        return rst
