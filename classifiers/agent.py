# -*- encoding = utf-8 -*-
import const.sql
from const import conf
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

STOPWORDS = {'iphone', 'app', 'springboard'}


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
                try:
                    value = plistObj[key].decode('ascii')
                except:
                    print plistObj[key], key
                    continue

            value = unescape(value.lower()).strip()
            if value.lower() not in STOPWORDS:
                features[key] = value
    return features


def load_lexical():
    appFeatures = load_info_features(_parse_xml)
    return appFeatures


def persist(appRule, ruleType):
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
        # if label in identifier:
        #     prefix = suffix = r'\b'
        params.append((host, prefix, identifier, suffix, label, 1, score, 3, consts.APP_RULE))

    sqldao.executeBatch(const.sql.SQL_INSERT_AGENT_RULES, params)
    sqldao.close()


class AgentClassifier(AbsClassifer):
    def __init__(self, inferFrmData=True):
        self.rules = defaultdict(dict)
        self.threshold = conf.agent_support

    @staticmethod
    def _app(potentialId, potentialHost, extractors):
        appRules = set()
        for _, extractor in extractors:
            if extractor.weight() <= conf.agent_support:
                continue

            for identifier, records in extractor.match2.items():
                for app, host in records:
                    if len(potentialId[identifier]) == 1:
                        r = consts.Rule(None, extractor.prefix.pattern, identifier, extractor.suffix.pattern, extractor.weight(), app)
                        appRules.add(r)

                    elif len(potentialId[identifier]) > 1 and len(potentialHost[host]) == 1:
                        r = consts.Rule(host, extractor.prefix.pattern, identifier, extractor.suffix.pattern, extractor.weight(), app)
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
        extractors = {}
        for appAgent in agentTuples.values():
            if conf.debug : print "[DEBUG]", appAgent
            for app, agent, host in appAgent:
                for key, v in sorted(appFeatures[app].items(), key=sortFunc, reverse=True):
                    if conf.debug: print "[DEBUG]", key, v
                    ifMatch = False
                    for value in filter(checkValue, self._gen_features(v)):
                        tmp = agent.replace(value, consts.IDENTIFIER, 1)
                        if tmp not in extractors: extractors[tmp] = Identifier(tmp)
                        extractors[tmp].add_identifier(app, value, host)
                        ifMatch = True
                        break
                    if ifMatch:
                        break
        return extractors

    def __count(self, agentTuples, extractors, appFeatures, lexicalIds):
        """
        Count regex
        :param appAgent: app -> (host, agent) -> tbls
        """
        print '[Agent175] threshold', conf.agent_support
        potentialId = defaultdict(set)
        for _, appAgent in agentTuples.items():
            for app, agent, host in appAgent:
                for key, extractor in extractors:
                    identifier = extractor.match(agent)
                    if identifier:
                        potentialId[identifier].add(app)
                        if extractor.weight() > conf.agent_support:
                            if '/' not in identifier and ':' not in identifier:
                                extractor.add_identifier(app, identifier, host)
        return potentialId


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
        if conf.debug: print "[DEBUG] number of lexicalId", len(lexicalIds)
        '''
        Compose regular expression
        '''
        extractors = self.__compose_idextractor(trainData, appFeatures)
        if conf.debug: print "[DEBUG] number of extractors", len(extractors)
        extractors = sorted(extractors.items(), key=lambda x: x[1].weight(), reverse=True)

        '''
        Count regex
        '''
        print 'Len extractors', len(extractors), 'Len agent', len(trainData)
        potentialId  = self.__count(trainData, extractors, appFeatures, lexicalIds)

        print "Finish Counter"

        appRule = self._app(potentialId, potentialHost, extractors)

        if ifPersist:
            persist(appRule, consts.APP_RULE)


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
        length = 0
        for host, prefix, identifier, suffix, label, support, confidence, ruleType, labelType in sqldao.execute(
                const.sql.SQL_SELECT_AGENT_RULES):
            counter += 1
            if identifier == 'mozilla':
                continue
            agentRegex = gen_regex(prefix, identifier, suffix, label)
            length += len(agentRegex)
            lexicalR = consts.Rule(host, prefix, identifier, suffix, confidence, label)
            if host is None:
                self.rules[labelType][lexicalR] = (re.compile(agentRegex), label)
            else:
                self.rulesHost[labelType][host][lexicalR] = (re.compile(agentRegex), label)
        print '>>> [Agent Rules#loadRules] total number of rules is', counter, 'Type of Rules', len(self.rules)
        print '>>> [Agent Rules#loadRules] Average Rule Length is', length * 1.0 / counter
        sqldao.close()

    def classify(self, testSet):
        compressed = defaultdict(lambda: defaultdict(set))
        for tbl, pkg in DataSetIter.iter_pkg(testSet):
            compressed[pkg.agent][pkg.rawHost].add(pkg)

        batchPredicts = {}
        for agent, host, pkgs in flatten(compressed):
            assert (type(pkgs) == set, "Type of pkgs is not correct" + str(type(pkgs)))
            predict = self.c((agent, host))
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
            matchRule = None
            rstLabel = None
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
            score = 0 if matchRule is None else matchRule.score
            rst[ruleType] = consts.Prediction(rstLabel, score, matchRule)
        return rst

    def classify2(self, testSet):
        def wrap_predict(predicts):
            wrapPredicts = {}
            for ruleType, predict in predicts.items():
                label, evidence = predict
                wrapPredicts[ruleType] = consts.Prediction(label, 1.0, evidence) if label else consts.NULLPrediction
            return wrapPredicts

        compressed = defaultdict(lambda: defaultdict(set))
        rt = defaultdict(lambda: defaultdict(set))
        for tbl, pkg in DataSetIter.iter_pkg(testSet):
            compressed[pkg.agent][pkg.rawHost].add(pkg)
            rt[pkg.agent][pkg.rawHost].add((pkg, tbl))

        batchPredicts = {}
        for agent, host, pkgs in flatten(compressed):
            assert (type(pkgs) == set, "Type of pkgs is not correct" + str(type(pkgs)))
            predict = wrap_predict(self.c((agent, host)))
            for pkg in pkgs:
                if predict[consts.APP_RULE].label == pkg.app:
                    batchPredicts[pkg.tbl + '#' + str(pkg.id)] = predict[consts.APP_RULE]
        return batchPredicts