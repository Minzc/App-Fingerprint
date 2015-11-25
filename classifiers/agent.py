# -*- encoding = utf-8 -*-
from utils import unescape, flatten, load_info_features
from sqldao import SqlDao
from collections import defaultdict
import const.consts as consts
from classifier import AbsClassifer
import re
import urllib
from const.dataset import DataSetIter as DataSetIter

VALID_FEATURES = {'CFBundleName', 'CFBundleExecutable', 'CFBundleIdentifier',
                  'CFBundleDisplayName', 'CFBundleURLSchemes'}
STRONG_FEATURES = {'CFBundleName', 'CFBundleExecutable', 'CFBundleIdentifier', 'CFBundleDisplayName'}


class FRegex:
    def __init__(self, featureStr, regexStr, rawF):
        self.featureStr = featureStr
        self.regexStr = regexStr
        self.rawF = rawF
        self.regexObj = re.compile(regexStr, re.IGNORECASE)
        self.matchRecord =  defaultdict(lambda: defaultdict(set))
        self.matchCategory = set()
        self.matchCompany = set()
        self.cover = set()

    def set_match_record(self, host, app, tbls, category, company):
        for tbl in tbls:
            self.matchRecord[host][app].add(tbl)
        self.matchCategory.add(category)
        self.matchCompany.add(company)

    def set_cover(self, regexSet):
        self.cover = regexSet



class AgentClassifier(AbsClassifer):
    def __init__(self, inferFrmData=True, sampleRate=1):
        self.rules = defaultdict(dict)
        self.appFeatures = load_info_features(self._parse_xml)
        self.inferFrmData = inferFrmData
        self.sampleRate = sampleRate
        '''Following variables are used to speed up the count step '''
        self.regexCover = defaultdict(set)

    @staticmethod
    def _parse_xml(filePath):
        import plistlib
        plistObj = plistlib.readPlist(filePath)
        features = {}
        for key in VALID_FEATURES:
            if key in plistObj:
                value = plistObj[key]
                if type(plistObj[key]) != unicode:
                    value = plistObj[key].decode('ascii')

                value = unescape(value.lower())
                features[key] = value
        return features

    def persist(self, appRule, companyRule, hostAgent, ruleType):
        """
        Input
        :param appRule : {regex: {app1, app2}}
        :param ruleType : type of rules (App, Company, Category)
        :param hostAgent: (host, regex) -> label
        """
        self.clean_db(ruleType, consts.SQL_DELETE_AGENT_RULES)
        sqldao = SqlDao()
        QUERY = consts.SQL_INSERT_AGENT_RULES
        params = []

        for fRegex, app in appRule.iteritems():
            assert type(fRegex.regexObj.pattern) == str
            assert type(app) == str
            params.append((app, 1, 1,fRegex.regexObj.pattern, '', consts.APP_RULE))

        for fRegex, company in companyRule.iteritems():
            assert type(fRegex.regexObj.pattern) == str
            assert type(company) == str
            params.append((company, 1, 1,fRegex.regexObj.pattern, '', consts.COMPANY_RULE))

        for rule, app in hostAgent.items():
            host, agentRegex = rule
            assert type(host) == str
            assert type(agentRegex) == str
            assert type(app) == str
            params.append((app, 1, 1, agentRegex, host, consts.APP_RULE))

        sqldao.executeBatch(QUERY, params)
        sqldao.close()

    # def _add_host(self, patterns, hostCategory):
    #     hostAgentRule = {}
    #     for fRegex, apps in patterns.iteritems():
    #         if len(apps) > 1 and fRegex.rawF is not None and len(fRegex.matchCategory) == 1:
    #             for host in fRegex.matchRecord:
    #                 if len(fRegex.matchRecord[host]) == 1 and len(hostCategory[host]) == 1:
    #                     hostAgentRule[(host, fRegex.regexObj.pattern)] = list(fRegex.matchRecord[host])[0]
    #
    #     return hostAgentRule

    def _company(self, patterns):
        companyRule = {}
        for fRegex, apps in patterns.iteritems():
            if len(apps) > 1 and fRegex.rawF is not None and len(fRegex.matchCompany) == 1:
                companyRule[fRegex] = list(fRegex.matchCompany)[0]
        return companyRule

    def _app(self, patterns, hostCategory):
        appRules = {}
        for fRegex, apps in filter(lambda item: len(item[1]) == 1, patterns.iteritems()):
            app = list(apps)[0]
            appRules[fRegex] = app

        hostAgentRule = {}
        for fRegex, apps in patterns.iteritems():
            if len(apps) > 1 and fRegex.rawF is not None and len(fRegex.matchCategory) == 1:
                for host in fRegex.matchRecord:
                    if len(fRegex.matchRecord[host]) == 1 and len(hostCategory[host]) == 1:
                        hostAgentRule[(host, fRegex.regexObj.pattern)] = list(fRegex.matchRecord[host])[0]
        return appRules, hostAgentRule

    @staticmethod
    def _gen_features(f):
        """
        Generate different type of feature
        """

        featureSet = set()
        featureSet.add(f)
        try:
            featureSet.add(urllib.quote(f))
        except:
            pass

        featureSet.add(f.replace(' ', '%20'))
        featureSet.add(f.replace(' ', '-'))
        featureSet.add(f.replace(' ', '_'))
        featureSet.add(f.replace(' ', ''))
        return featureSet

    def _gen_regex(self, featureStr):
        if featureStr[-1].isalnum() == False:
            featureStr = featureStr[:-1]
        if featureStr[0].isalnum() == False:
            featureStr = featureStr[1:]

        regex = []
        regexStr1 = r'^' + re.escape(featureStr + '/')
        regexStr2 = r'\b' + re.escape(featureStr) + r' \b[vr]?[0-9.]+\b'
        regexStr3 = r'\b' + re.escape(featureStr + '/')
        regexStr4 = r'\b' + re.escape(featureStr) + r'\b'
        regex.append(regexStr1)
        regex.append(regexStr2)
        regex.append(regexStr3)
        regex.append(regexStr4)
        self.regexCover[regexStr1].add(regexStr3)
        self.regexCover[regexStr1].add(regexStr4)
        self.regexCover[regexStr2].add(regexStr4)
        self.regexCover[regexStr3].add(regexStr4)
        return regex

    def _compose_regxobj(self, agentTuples):
        def _compile_regex():
            for featureStr in self._gen_features(f):

                '''1. featureStr in agent. 2. featureStr is app'''
                if len(filter(lambda agent: featureStr in agent, agents)) > 0 or app in featureStr:

                    for regexStr in self._gen_regex(featureStr):
                        appFeatureRegex[app][regexStr] = FRegex(featureStr, regexStr, f)

            for agent in filter(lambda x: '/' in x, agents):
                matchStrs = re.findall(r'^[a-zA-Z0-9][0-9a-zA-Z. _\-:&?\'%!]+/', agent)
                if len(matchStrs) > 0:
                    regexStr = r'^' + re.escape(matchStrs[0])
                    if regexStr not in appFeatureRegex[app]:
                        try:
                            featureStr = matchStrs[0]
                            appFeatureRegex[app][regexStr] = FRegex(featureStr, regexStr, None)
                        except:
                            pass

        '''
        Compose regular expression
        Only use apps occurred in agentTuples
        '''
        appFeatureRegex = defaultdict(lambda: {})
        for app, agents in agentTuples.items():
            for f in self.appFeatures[app].values():
                _compile_regex()

        return appFeatureRegex

    def _prune(self, regexLabel):
        """
        :param regexLabel: FRegex -> apps
        :return:
        """

        def sortPattern(regexAppItem):
            fRgex, apps = regexAppItem
            f = fRgex.regexStr
            if f in invRegexCover:
                return len(invRegexCover[f])
            else:
                return 0

        invRegexCover = defaultdict(set)
        for regexStr, regexStrs in self.regexCover.items():
            for str in regexStrs:
                invRegexCover[str].add(regexStr)
        regexLabel = sorted(regexLabel.items(), key=sortPattern, reverse=True)
        rst = defaultdict(set)
        pruned = defaultdict(set)
        for fRegex, apps in regexLabel:
            apps = frozenset(apps)
            for regexStr in invRegexCover[fRegex.regexStr]:
                pruned[apps].add(regexStr)
            if fRegex.regexStr not in pruned[apps]:
                rst[fRegex] = apps
        return rst

    def _count(self, appFeatureRegex, appAgent, trainapps, appCategory, appCompany):
        """
        Count regex
        :param appAgent: app -> (host, agent) -> tbls
        """

        def sortPattern(regexTuples):
            _, f, _ = regexTuples
            if f in self.regexCover:
                return len(self.regexCover[f])
            else:
                return 0

        '''Flatten appFeature so that it's earsier to iterate'''
        fAppFeatureRegex = sorted(flatten(appFeatureRegex), key=sortPattern, reverse=True)
        '''
        Some useful features are not detected due to data distribution
        Add prediction to relationships
        '''
        regexApp = defaultdict(set)

        for predict, regexStr, fRegex in filter(lambda x: x[0] not in trainapps, fAppFeatureRegex):
            regexApp[fRegex].add(predict)

        for agent, values in appAgent.items():
            covered = set()
            apps = set(values.keys())

            for predict, regexStr, fRegex in fAppFeatureRegex:
                if fRegex.featureStr not in agent and fRegex.featureStr != predict:
                    continue
                if regexStr in covered or fRegex.regexObj.search(agent):
                    regexApp[fRegex] |= apps
                    for app in apps:
                        for host in values[app]:
                            fRegex.set_match_record(host, app, values[app][host], appCategory[app], appCompany[app])
                    for regex in self.regexCover[regexStr]:
                        covered.add(regex)
                elif fRegex.featureStr in apps:
                    app = fRegex.featureStr
                    regexApp[fRegex].add(app)
                    for host in values[app]:
                        fRegex.set_match_record(host, app, values[app][host], appCategory[app], appCompany[app])
        return regexApp

    def _infer_from_xml(self, appFeatureRegex, agentTuples):
        for app, features in filter(lambda x: x[0] not in agentTuples, self.appFeatures.items()):
            for f in features.values():
                for featureStr in self._gen_features(f):
                    for regexStr in self._gen_regex(featureStr):
                        appFeatureRegex[app][regexStr] = FRegex(featureStr, regexStr, f)


    def train(self, trainSet, ruleType):
        agentTuples = defaultdict(set)
        cmprsDB = defaultdict(lambda: defaultdict(lambda: defaultdict(set)))
        hostCategory = defaultdict(set)
        appCategory = dict()
        appCompany = dict()
        for tbl, pkg in DataSetIter.iter_pkg(trainSet):
            label, agent = pkg.label, pkg.agent
            agentTuples[label].add(agent)
            cmprsDB[agent][label][pkg.host].add(tbl)
            hostCategory[pkg.host].add(pkg.category)
            appCategory[label] = pkg.category
            appCompany[label] = pkg.company

        '''
        Compose regular expression
        '''
        appFeatureRegex = self._compose_regxobj(agentTuples)

        print 'Infer From Data Is', self.inferFrmData
        if self.inferFrmData:
            self._infer_from_xml(appFeatureRegex, agentTuples)

        '''
        Count regex
        '''
        regexApp = self._count(appFeatureRegex, cmprsDB, set(agentTuples.keys()), appCategory, appCompany)

        print "Finish Counter"

        regexApp = self._prune(regexApp)
        companyRule = self._company(regexApp)
        appRule, hostAgent = self._app(regexApp, hostCategory)
        print 'Company Rules', len(companyRule)

        print "Finish Pruning"

        # hostAgent = self._add_host(regexApp, hostCategory)
        hostAgent = self.change_raw(hostAgent, trainSet)

        self.persist(regexApp, companyRule, hostAgent, consts.APP_RULE)



    def load_rules(self):
        self.rules = {consts.APP_RULE: {}, consts.COMPANY_RULE: {}, consts.CATEGORY_RULE: {}}
        self.rulesHost = {consts.APP_RULE: defaultdict(dict),
                          consts.COMPANY_RULE: defaultdict(dict),
                          consts.CATEGORY_RULE: defaultdict(dict)}
        QUERY = consts.SQL_SELECT_AGENT_RULES
        sqldao = SqlDao()
        counter = 0
        for host, agentF, label, ruleType in sqldao.execute(QUERY):
            counter += 1
            if host == '':
                self.rules[ruleType][agentF] = (re.compile(agentF), label)
            else:
                self.rulesHost[ruleType][host][agentF] = (re.compile(agentF), label)
        print '>>> [Agent Rules#loadRules] total number of rules is', counter, 'Type of Rules', len(self.rules)
        sqldao.close()

    def classify(self, testSet):
        def wrap_predict(predicts):
            wrapPredicts = {}
            for ruleType, predict in predicts.items():
                label, evidence = predict
                wrapPredicts[ruleType] = consts.Prediction(label, 1.0, evidence) if label else consts.NULLPrediction
            return wrapPredicts

        compressed = defaultdict(lambda : defaultdict(set))
        for tbl, pkg in DataSetIter.iter_pkg(testSet):
            compressed[pkg.agent][pkg.rawHost].add(pkg)

        batchPredicts = {}
        for agent, host, pkgs in flatten(compressed):
            assert(type(pkgs) == set, "Type of pkgs is not correct" + str(type(pkgs)))
            predict = wrap_predict(self.c((agent, host)))
            for pkg in pkgs:
                batchPredicts[pkg.id] = predict
                l = predict[consts.APP_RULE].label
                if l is not None and l != pkg.app:
                    print '>>>[AGENT CLASSIFIER ERROR] agent:', pkg.agent, 'App:', pkg.app, 'Prediction:', predict[consts.APP_RULE]
        return batchPredicts

    def c(self, pkgInfo):
        agent, host = pkgInfo
        rst = {}
        for ruleType in self.rules:
            longestWord = ''
            rstLabel = None
            for agentF, regxNlabel in self.rules[ruleType].items():
                regex, label = regxNlabel
                if regex.search(agent) and len(longestWord) < len(agentF):
                    rstLabel = label
                    longestWord = agentF

            for agentF, regxNlabel in self.rulesHost[ruleType][host].items():
                regex, label = regxNlabel
                if regex.search(agent) and len(longestWord) < len(agentF):
                    rstLabel = label
                    longestWord = agentF
            rst[ruleType] = (rstLabel, longestWord)
        return rst

    def change_raw(self, rules, trainSet):
        tmpRules = {}
        hostDict = {}
        for rule, app in rules.items():
            hostDict[rule[0]] = set()

        for tbl, pkg in DataSetIter.iter_pkg(trainSet):
            if pkg.host in hostDict:
                hostDict[pkg.host].add(pkg.rawHost)

        for rule, app in rules.items():
            host, regexObj = rule
            for rawHost in hostDict[host]:
                tmpRules[(rawHost, regexObj)] = app
        return tmpRules
