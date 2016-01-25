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


def gen_regex(prefix, identifier, suffix, app):
    if identifier != app:
        return prefix + re.escape(identifier) + suffix
    else:
        return re.escape(identifier)


class Identifier:
    def __init__(self, rule):
        start = rule.find(consts.IDENTIFIER)
        end = start + len(consts.IDENTIFIER)
        prefix = r'^' + re.escape(rule[:start])
        suffix = re.escape(rule[end:]) + '$'
        self.ruleStr = rule
        self.prefix = re.compile(prefix)
        self.suffix = re.compile(suffix)
        self.matched = defaultdict(set)
        self.match2 = defaultdict(set)
        self.apps = set()

    def match(self, agent):
        if self.prefix.search(agent) and self.suffix.search(agent):
            agent = self.prefix.sub('', agent)
            agent = self.suffix.sub('', agent)
        else:
            agent = None
        return agent

    def add_identifier(self, app, identifier, host):
        self.apps.add(app)
        self.matched[identifier].add(app)
        self.match2[identifier].add((app, host))

    def weight(self):
        return len(self.apps)

    def check(self, identifier):
        return identifier in self.matched


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


def load_lexical():
    appFeatures = load_info_features(_parse_xml)
    return appFeatures


def persist(appRule, ruleType, lexicalIds):
    def convert_regex(regexStr):
        regexStr = regexStr.replace(re.escape(consts.VERSION), r'\b[a-z0-9-.]+\b')
        regexStr = regexStr.replace(re.escape(consts.RANDOM), r'[0-9a-z]*')
        return regexStr

    """
    Input
    :param companyRule:
    :param appRule : {regex: {app1, app2}}
    :param ruleType : type of prune (App, Company, Category)
    :param hostAgent: (host, regex) -> label
    """
    sqldao = SqlDao()
    params = []

    for rule in appRule:
        host, prefix, identifier, suffix, score, label = rule
        prefix = convert_regex(prefix)
        identifier = convert_regex(identifier)
        suffix = convert_regex(suffix)
        params.append((host, prefix, identifier, suffix, label, 1, score, 3, consts.APP_RULE))

    sqldao.executeBatch(consts.SQL_INSERT_AGENT_RULES, params)
    sqldao.close()


class AgentBLClassifier(AbsClassifer):
    def __init__(self, inferFrmData=True):
        self.rules = defaultdict(dict)

    @staticmethod
    def _app(potentialId, potentialHost, extractors):
        appRules = set()
        for _, extractor in extractors:
            for identifier, records in extractor.match2.items():
                for app, host in records:
                    if len(potentialId[identifier]) == 1:
                        r = consts.Rule(None, extractor.prefix.pattern, identifier, extractor.suffix.pattern, 100, app)
                        appRules.add(r)

                    elif len(potentialId[identifier]) > 1 and len(potentialHost[host]) == 1:
                        r = consts.Rule(host, extractor.prefix.pattern, identifier, extractor.suffix.pattern, 100, app)
                        appRules.add(r)

        # for identifier in check:
        #     print '[CHECK]',identifier, identifierApps[identifier]
        return appRules

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

    def __compose_idextractor(self, agentTuples, appFeatures):
        sortFunc = lambda x: len(x[1])
        checkValue = lambda value: value not in STOPWORDS and value in agent
        matchedValue = set()
        extractors = {}
        for appAgent in agentTuples.values():
            for app, agent, host in appAgent:
                for key, v in sorted(appFeatures[app].items(), key=sortFunc, reverse=True):
                    ifMatch = False
                    for value in filter(checkValue, self._gen_features(v)):
                        matchedValue.add(value)
                        tmp = agent.replace(value, consts.IDENTIFIER, 1)
                        if tmp not in extractors: extractors[tmp] = Identifier(tmp)
                        extractors[tmp].add_identifier(app, value, host)
                        ifMatch = True
                        break
                    if ifMatch:
                        break
        return extractors, matchedValue

    def __count(self, agentTuples, extractors, appFeatures, lexicalIds):
        """
        Count regex
        :param appAgent: app -> (host, agent) -> tbls
        """
        potentialId = defaultdict(set)
        for _, appAgent in agentTuples.items():
            for app, agent, host in appAgent:
                for key, extractor in extractors:
                    identifier = extractor.match(agent)
                    if identifier:
                        potentialId[identifier].add(app)

        return potentialId

    # def _infer_from_xml(self, appFeatureRegex, agentTuples):
    #     for app, features in filter(lambda x: x[0] not in agentTuples, self.appFeatures.items()):
    #         for f in features.values():
    #             if len(self.valueApp[f]) == 1 and f not in STOPWORDS:
    #                 for featureStr in self._gen_features(f):
    #                     for regexStr in self._gen_regex(featureStr):
    #                         appFeatureRegex[app][regexStr] = FRegex(featureStr, regexStr, f)

    def train(self, trainSet, ruleType, ifPersist=True):
        trainData = defaultdict(set)
        potentialHost = defaultdict(set)
        for tbl, pkg in DataSetIter.iter_pkg(trainSet):
            trainData[tbl].add((pkg.app, process_agent(pkg.agent, pkg.app), pkg.host))
            potentialHost[pkg.host].add(pkg.app)

        appFeatures = load_lexical()
        lexicalIds = set()
        for appLexicalId in appFeatures.values():
            for lexicalId in appLexicalId.values():
                lexicalIds.add(lexicalId)

        '''
        Compose regular expression
        '''
        extractors, matchedValue = self.__compose_idextractor(trainData, appFeatures)
        extractors = sorted(extractors.items(), key=lambda x: x[1].weight(), reverse=True)

        for value in matchedValue:
            lexicalIds.add(value)

        '''
        Count regex
        '''
        print 'Len extractors', len(extractors), 'Len agent', len(trainData)
        potentialId  = self.__count(trainData, extractors, appFeatures, lexicalIds)

        print "Finish Counter"

        # identifierApps, extractors = self._prune(regexApp)
        appRule = self._app(potentialId, potentialHost, extractors)

        if ifPersist:
            print "Finish Pruning"
            persist(appRule, consts.APP_RULE, lexicalIds)

    def load_rules(self):
        self.rules = {
            consts.APP_RULE: {},
            consts.COMPANY_RULE: {},
            consts.CATEGORY_RULE: {}
        }
        self.rulesHost = {
            consts.APP_RULE: defaultdict(dict),
            consts.COMPANY_RULE: defaultdict(dict),
            consts.CATEGORY_RULE: defaultdict(dict)
        }

        sqldao = SqlDao()
        counter = 0
        for host, prefix, identifier, suffix, label, support, confidence, ruleType, labelType in sqldao.execute(
                consts.SQL_SELECT_AGENT_RULES):
            counter += 1
            agentRegex = gen_regex(prefix, identifier, suffix, label)
            lexicalR = consts.Rule(host, prefix, identifier, suffix, support, label)
            if host is None:
                self.rules[labelType][lexicalR] = (re.compile(agentRegex), label)
            else:
                self.rulesHost[labelType][host][lexicalR] = (re.compile(agentRegex), label)
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
            for lexicalR, regxNlabel in self.rules[ruleType].items():
                regex, label = regxNlabel
                if regex.search(agent) and len(longestWord) < len(lexicalR.identifier):
                    rstLabel = label
                    longestWord = lexicalR.identifier

            for lexicalR, regxNlabel in self.rulesHost[ruleType][host].items():
                regex, label = regxNlabel
                if regex.search(agent) and len(longestWord) < len(lexicalR.identifier):
                    rstLabel = label
                    longestWord = lexicalR.identifier
            rst[ruleType] = (rstLabel, longestWord)
        return rst

    def p(self, pkgInfo):
        agent, host = pkgInfo
        rst = {}
        for ruleType in self.rules:
            longestWord = ''
            rstLabel = None
            matchRule = None
            for lexicalR, regxNlabel in self.rules[ruleType].items():
                regex, label = regxNlabel
                if regex.search(agent) and len(longestWord) < len(lexicalR.identifier):
                    rstLabel = label
                    longestWord = lexicalR.identifier
                    matchRule = lexicalR

            for lexicalR, regxNlabel in self.rulesHost[ruleType][host].items():
                regex, label = regxNlabel
                if regex.search(agent) and len(longestWord) < len(lexicalR.identifier):
                    rstLabel = label
                    longestWord = lexicalR.identifier
                    matchRule = lexicalR

            rst[ruleType] = (rstLabel, matchRule)
        return rst




        # @staticmethod
        # def change_raw(prune, trainSet):
        #     tmpRules = {}
        #     hostDict = {}
        #     for rule, app in prune.items():
        #         hostDict[rule[0]] = set()
        #
        #     for tbl, pkg in DataSetIter.iter_pkg(trainSet):
        #         if pkg.host in hostDict:
        #             hostDict[pkg.host].add(pkg.rawHost)
        #
        #     for rule, app in prune.items():
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
