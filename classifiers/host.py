from utils import longest_common_substring, get_top_domain, url_clean, load_exp_app
from sqldao import SqlDao
from collections import defaultdict
import const.consts as consts
from const.app_info import AppInfos
from classifier import AbsClassifer
import re

test_str = {'stats.3sidedcube.com', 'redcross.com'}


class HostApp(AbsClassifer):
    def __init__(self, appType):
        self.appType = appType
        self.urlLabel = defaultdict(set)
        self.substrCompany = defaultdict(set)
        self.labelAppInfo = {}
        self.rules = defaultdict(dict)

    def persist(self, patterns, rule_type):
        self._clean_db(rule_type)
        sqldao = SqlDao()
        QUERY = consts.SQL_INSERT_HOST_RULES
        params = []
        for ruleType in patterns:
            for url, labelNsupport in patterns[ruleType].iteritems():
                label, support = labelNsupport
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
        segApps = defaultdict(set)
        for label, appInfo in expApp.iteritems():
            appSegs = appInfo.package.split('.')
            companySegs = appInfo.company.split(' ')
            categorySegs = appInfo.category.split(' ')
            websiteSegs = url_clean(appInfo.website).split('.')
            wholeSegs = [appSegs, companySegs, categorySegs, websiteSegs]
            for segs in wholeSegs:
                for seg in segs:
                    self.fLib[label].add(seg)
                    segApps[seg].add(label)
        for label, segs in self.fLib.items():
            self.fLib[label] = {seg for seg in segs if len(segApps[seg]) == 1}


    def train(self, records, rule_type):
        expApp = load_exp_app()[self.appType]
        expApp = {label: AppInfos.get(self.appType, label) for label in expApp}
        self._feature_lib(expApp)
        for pkgs in records.values():
            for pkg in pkgs:
                self.count(pkg)
        ########################
        # Generate Rules
        ########################

        for url, labels in self.urlLabel.iteritems():
            if url in test_str:
                print '#', len(labels)
                print labels
                print url

            if len(labels) == 1:
                label = list(labels)[0]
                ifValidRule = self._check(url, label, expApp[label].website)
                ifValidRule = ifValidRule | self._check(url, label, label)

                if ifValidRule:
                    self.rules[rule_type][url] = (label, set())

                if url in test_str:
                    print 'Rule Type is', rule_type, ifValidRule, url

        print 'number of rule', len(self.rules[consts.APP_RULE])

        self.count_support(records)
        self.persist(self.rules, rule_type)
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

    def count_support(self, records):
        LABEL = 0
        TBLSUPPORT = 1
        for tbl, pkgs in records.items():
            for pkg in pkgs:
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

    def classify(self, pkg):
        """
        Input
        - self.rules : {ruleType: {host : (label, support, regexObj)}}
        :param pkg: http packet
        """
        rst = {}
        for ruleType in self.rules:
            predict = consts.NULLPrediction
            for regexStr, ruleTuple in self.rules[ruleType].iteritems():
                label, support, regexObj = ruleTuple
                #host = pkg.refer_host if pkg.refer_host else pkg.host
                host = pkg.host
                match = regexObj.search(host)
                if match and predict.score < support:
                    predict = consts.Prediction(label, support, (host, regexStr, support))
                    # if pkg.app == 'com.logos.vyrso' and pkg.host == 'gsp1.apple.com':
                    #   print regexStr
                    # print match

            rst[ruleType] = predict
            if predict.label != pkg.app and predict.label is not None:
                print 'Evidence:', predict.evidence, 'App:', pkg.app, 'Predict:', predict.label
        return rst
