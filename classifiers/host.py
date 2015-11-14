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

    def count(self, pkg):
        def addCommonStr(url, label, string):
            common_str = longest_common_substring(url.lower(), string.lower())
            common_str = common_str.strip('.')
            #print common_str, url, string, label
            if common_str not in self.fLib[label]:
                return
            # for subStr in filter( lambda x: common_str in x, string.split('.')):
            #     sPos = subStr.find(common_str)
            #     ePos = sPos + len(common_str)
            #     if sPos != 0 and ePos != len(subStr):
            #         return
            self.substrCompany[common_str].add(pkg.label)


        host = url_clean(pkg.host)
        refer_host = pkg.refer_host
        if not host:
            return

        self.labelAppInfo[pkg.label] = [pkg.website]
        map(lambda url: self.urlLabel[url].add(pkg.label), [host, refer_host])
        map(lambda string: addCommonStr(host, pkg.label, string), [pkg.website])

    def checkCommonStr(self, label, url):
        for astr in self.labelAppInfo[label]:
            common_str = longest_common_substring(url.lower(), astr.lower())
            common_str = common_str.strip('.')
            if url in test_str:
                print common_str, url,astr
                print self.substrCompany[common_str], url
            subCompanyLen = len(self.substrCompany[common_str])
            strValid = True if len(common_str) > 2 else False
            companyValid = True if 5 > subCompanyLen > 0 else False

            if companyValid and strValid:
                if url in test_str:
                    print 'INNNNNNNNNNNN', url, label, common_str
                return True
        return False

    @staticmethod
    def _clean_db(rule_type):
        QUERY = consts.SQL_DELETE_HOST_RULES
        sqldao = SqlDao()
        sqldao.execute(QUERY % rule_type)
        sqldao.close()

    def _feature_lib(self, expApp):
        self.fLib = defaultdict(set)
        for label, appInfo in expApp.iteritems():
            appSegs = appInfo.package.split('.')
            companySegs = appInfo.company.split(' ')
            categorySegs = appInfo.category.split(' ')
            websiteSegs = url_clean(appInfo.website).split('.')
            print websiteSegs
            wholeSegs = [appSegs, companySegs, categorySegs, websiteSegs]
            for segs in wholeSegs:
                for seg in segs:
                    self.fLib[label].add(seg)

    def train(self, records, rule_type):
        expApp = load_exp_app()[self.appType]
        expApp = {label: AppInfos.get(self.appType, label) for label in expApp}
        self._feature_lib(expApp)
        for pkgs in records.values():
            for pkg in pkgs:
                self.count(pkg)
        self._recount(records)
        ########################
        # Generate Rules
        ########################

        for url, labels in self.urlLabel.iteritems():
            if url in test_str:
                print '#', len(labels)
                print labels
                print url

            if len(labels) == 1:
                label = labels.pop()
                ifValidRule = True if self.checkCommonStr(label, url) else False

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
                    for url in [host,  refer_host]:
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
                host = pkg.refer_host if pkg.refer_host else pkg.host
                if pkg.app == 'com.idrudgereport.idrudgereportuniversal' and pkg.host == 'images.politico.com':
                    print '>>>', host, pkg.refer_host, host
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
