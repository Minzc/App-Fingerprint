# -*- encoding = utf-8 -*-
from utils import unescape, flatten, load_info_features, process_agent
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

STOPWORDS = {'iphone', 'app'}

class FRegex:
    def __init__(self, featureStr, regexStr, rawF):
        self.featureStr = featureStr
        self.regexStr = regexStr
        self.rawF = rawF
        self.regexObj = re.compile(regexStr, re.IGNORECASE)
        self.matchRecord = defaultdict(lambda: defaultdict(set))
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

class Identifier:
    def __init__(self, rule):
        start = rule.find(consts.IDENTIFIER)
        end = start + len(consts.IDENTIFIER)
        prefix = r'^' + re.escape(rule[:start])
        suffix = re.escape(rule[end:])+'$'
        self.ruleStr = rule
        self.prefix = re.compile(prefix)
        self.suffix = re.compile(suffix)
        self.matched = defaultdict(set)
        self.apps = set()

    def match(self, agent):
        if self.prefix.search(agent) and self.suffix.search(agent):
            agent = self.prefix.sub('', agent)
            agent = self.suffix.sub('', agent)
        else:
            agent = None
        return agent

    def add_identifier(self, app, identifier):
        self.apps.add(app)
        self.matched[identifier].add(app)

    def weight(self):
        return len(self.apps)

    def check(self, identifier):
        return identifier in self.matched

    def gen(self, identifier, app):
        if identifier != app:
            return self.prefix.pattern + re.escape(identifier) + self.suffix.pattern
        else:
            return re.escape(identifier)

    # def match(self, agent):
    #     identifier = None
    #     maxLen = len(agent)
    #     for m in self.regex.finditer(agent):
    #         if len(m.group(1)) < maxLen:
    #             identifier = m.group(1)
    #             maxLen = len(identifier)
    #     return identifier
class AgentClassifier(AbsClassifer):
    def __init__(self, inferFrmData=True):
        self.rules = defaultdict(dict)
        self.appFeatures = load_info_features(self._parse_xml)

        self.valueApp = defaultdict(set)
        for app, features in self.appFeatures.items():
            for f in features.values():
                self.valueApp[f].add(app)

        self.inferFrmData = inferFrmData

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

                value = unescape(value.lower()).strip()
                if value.lower() not in STOPWORDS:
                    features[key] = value
        return features

    def persist(self, appRule, ruleType):
        """
        Input
        :param companyRule:
        :param appRule : {regex: {app1, app2}}
        :param ruleType : type of rules (App, Company, Category)
        :param hostAgent: (host, regex) -> label
        """
        self.clean_db(ruleType, consts.SQL_DELETE_AGENT_RULES)
        sqldao = SqlDao()
        QUERY = consts.SQL_INSERT_AGENT_RULES
        params = []

        for regexStr, app in appRule.iteritems():
            regexStr = regexStr.repace('[VERSION]', r'\b[a-z0-9-.]+\b')
            params.append((app, 1, 1, regexStr, '', consts.APP_RULE))

        sqldao.executeBatch(QUERY, params)
        sqldao.close()

    @staticmethod
    def _company(patterns):
        companyRule = {}
        for fRegex, apps in patterns.iteritems():
            if len(apps) > 1 and fRegex.rawF is not None and len(fRegex.matchCompany) == 1:
                companyRule[fRegex] = list(fRegex.matchCompany)[0]
        return companyRule

    @staticmethod
    def _app(identifierApps, extractors):
        appRules = {}
        hostAgentRule = {}

        check = set()
        for _, extractor in extractors:
            for identifier, apps in extractor.matched.items():
                if len(apps) == 1:
                    app = list(apps)[0]
                    appRules[extractor.gen(identifier, app)] = app
                    if len(identifierApps[identifier]) > 1:
                        check.add(identifier)

        for identifier in check:
            print '[CHECK]',identifier, identifierApps[identifier]
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


    def __compose_idextractor(self, agentTuples):
        sortFunc = lambda x:len(x[1])
        checkValue = lambda value : value not in STOPWORDS and value in agent

        extractors = {}
        for appAgent in agentTuples.values():
            for app, agent in appAgent:
                for key, v in sorted(self.appFeatures[app].items(), key=sortFunc, reverse=True):
                    ifMatch = False
                    for value in filter(checkValue, self._gen_features(v)):
                        tmp = agent.replace(value, consts.IDENTIFIER, 1)
                        # prefix, suffix = self.getPrefixNSuffix(agent)
                        if tmp not in extractors: extractors[tmp] = Identifier(tmp)
                        extractors[tmp].add_identifier(app, value)
                        ifMatch = True
                        break
                    if ifMatch == True:
                        break
        return extractors


    def __count(self, agentTuples, extractors):
        """
        Count regex
        :param appAgent: app -> (host, agent) -> tbls
        """

        identifierApps = defaultdict(set)
        notDisAgent = set()
        for _, appAgent in agentTuples.items():
            for app, agent in appAgent:
                ifMatch = False
                for key, extractor in filter(lambda x: x[1].weight() > 10, extractors):
                    identifier = extractor.match(agent)
                    if identifier:
                        ifMatch = True
                        extractor.add_identifier(app, identifier)
                        identifierApps[identifier].add(app)

                if ifMatch == False:
                    notDisAgent.add(agent)
        return identifierApps, extractors

    # def _infer_from_xml(self, appFeatureRegex, agentTuples):
    #     for app, features in filter(lambda x: x[0] not in agentTuples, self.appFeatures.items()):
    #         for f in features.values():
    #             if len(self.valueApp[f]) == 1 and f not in STOPWORDS:
    #                 for featureStr in self._gen_features(f):
    #                     for regexStr in self._gen_regex(featureStr):
    #                         appFeatureRegex[app][regexStr] = FRegex(featureStr, regexStr, f)

    def train(self, trainSet, ruleType):
        agentTuples = defaultdict(set)
        for tbl, pkg in DataSetIter.iter_pkg(trainSet):
            agentTuples[tbl].add((pkg.app, process_agent(pkg.agent, pkg.app)))

        '''
        Compose regular expression
        '''
        extractors = self.__compose_idextractor(agentTuples)
        extractors = sorted(extractors.items(), key=lambda x: x[1].weight(), reverse=True)


        print 'Infer From Data Is', self.inferFrmData
        # if self.inferFrmData:
        #     self._infer_from_xml(appFeatureRegex, agentTuples)

        '''
        Count regex
        '''
        print 'Len extractors', len(extractors), 'Len agent', len(agentTuples)
        identifierApps, extractors = self.__count(agentTuples, extractors)

        print "Finish Counter"

        # identifierApps, extractors = self._prune(regexApp)
        appRule, hostAgent = self._app(identifierApps, extractors)


        print "Finish Pruning"

        self.persist(appRule, consts.APP_RULE)

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

        compressed = defaultdict(lambda: defaultdict(set))
        for tbl, pkg in DataSetIter.iter_pkg(testSet):
            compressed[pkg.agent][pkg.rawHost].add(pkg)

        batchPredicts = {}
        for agent, host, pkgs in flatten(compressed):
            assert (type(pkgs) == set, "Type of pkgs is not correct" + str(type(pkgs)))
            predict = wrap_predict(self.c((agent, host)))
            for pkg in pkgs:
                batchPredicts[pkg.id] = predict
                l = predict[consts.APP_RULE].label
                if l is not None and l != pkg.app:
                    print '>>>[AGENT CLASSIFIER ERROR] agent:', pkg.agent, 'App:', pkg.app, 'Prediction:', predict[
                        consts.APP_RULE]
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




    # @staticmethod
    # def change_raw(rules, trainSet):
    #     tmpRules = {}
    #     hostDict = {}
    #     for rule, app in rules.items():
    #         hostDict[rule[0]] = set()
    #
    #     for tbl, pkg in DataSetIter.iter_pkg(trainSet):
    #         if pkg.host in hostDict:
    #             hostDict[pkg.host].add(pkg.rawHost)
    #
    #     for rule, app in rules.items():
    #         host, regexObj = rule
    #         for rawHost in hostDict[host]:
    #             tmpRules[(rawHost, regexObj)] = app
    #     return tmpRules



   #
    # def _prune(self, regexLabel):
    #     """
    #     :param regexLabel: FRegex -> apps
    #     :return:
    #     """
    #
    #     def sortPattern(regexAppItem):
    #         fRgex, apps = regexAppItem
    #         f = fRgex.regexStr
    #         if f in invRegexCover:
    #             return len(invRegexCover[f])
    #         else:
    #             return 0
    #
    #     invRegexCover = defaultdict(set)
    #     for regexStr, regexStrs in self.regexCover.items():
    #         for string in regexStrs:
    #             invRegexCover[string].add(regexStr)
    #     regexLabel = sorted(regexLabel.items(), key=sortPattern, reverse=True)
    #     rst = defaultdict(set)
    #     pruned = defaultdict(set)
    #     for fRegex, apps in regexLabel:
    #         apps = frozenset(apps)
    #         for regexStr in invRegexCover[fRegex.regexStr]:
    #             pruned[apps].add(regexStr)
    #         if fRegex.regexStr not in pruned[apps]:
    #             rst[fRegex] = apps
    #     return rst