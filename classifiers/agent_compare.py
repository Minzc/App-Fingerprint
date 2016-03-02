# -*- encoding = utf-8 -*-

import const.sql
from const import conf
from utils import flatten, process_agent
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

SPLITTER = re.compile("[" + r'''!"#$%&'()*+,\-/:;<=>?@[\]^_`{|}~ ''' + "]")


def gen_regex(prefix, identifier, suffix, app):
    if identifier != app:
        return prefix + re.escape(identifier) + suffix
    else:
        return re.escape(identifier)


class Identifier:
    def __init__(self, prefix, suffix):
        self.ruleStr = prefix + '|' + suffix
        prefix = prefix.replace('VERSION', consts.VERSION)
        suffix = suffix.replace('VERSION', consts.VERSION)

        if '^' == prefix[0]:
            prefix = '^' + re.escape(prefix[1:])
        else:
            prefix = re.escape(prefix)

        if len(prefix) > 1:
            prefix = prefix + '\b'

        if '$' == suffix[-1]:
            suffix = re.escape(suffix[:-1]) + '$'
        else:
            suffix = re.escape(suffix)

        self.prefix = re.compile(prefix)
        self.suffix = re.compile(suffix)
        self.identifier = re.compile(prefix + '(.+?)' + suffix)
        self.matched = defaultdict(set)
        self.match2 = defaultdict(set)
        self.apps = set()

    def match(self, agent):
        if self.prefix.search(agent) and self.suffix.search(agent):
            m = self.identifier.search(agent)
            if m:
                agent = m.group(1)
            else:
                agent = None
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
        if label in identifier:
            prefix = suffix = r'\b'
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
                    tmp = SPLITTER.sub('', identifier)
                    if len(potentialId[tmp]) == 1:
                        r = consts.Rule(None, extractor.prefix.pattern, identifier, extractor.suffix.pattern, 100, app)
                        appRules.add(r)
                    # elif len(potentialId[identifier]) > 1 and len(potentialHost[host]) == 1:
                    #     r = consts.Rule(host, extractor.prefix.pattern, identifier, extractor.suffix.pattern, 100, app)
                    #     appRules.add(r)

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


    def __check(self, identifier, unticorr):
        for w in unticorr:
            if w in identifier:
                print('##', identifier, w)
                return None
        if '/' in identifier[:-1]:
            return None
        if ':' in identifier:
            return None
        if ';' in identifier[:-1]:
            return None
        return identifier

    def __count(self, agentTuples, extractors, unticorr):
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
                    # if 'emergencyradiofree' in agent:
                    #     print('###', extractor.identifier.pattern, identifier, agent)
                    if identifier:
                        tmp = SPLITTER.sub('', identifier)
                        potentialId[tmp].add(app)
                        if extractor.weight() > conf.agent_support:
                            if self.__check(identifier, unticorr[app]):
                                extractor.add_identifier(app, identifier, host)
                                break
        return potentialId

    # def _infer_from_xml(self, appFeatureRegex, agentTuples):
    #     for app, features in filter(lambda x: x[0] not in agentTuples, self.appFeatures.items()):
    #         for f in features.values():
    #             if len(self.valueApp[f]) == 1 and f not in STOPWORDS:
    #                 for featureStr in self._gen_features(f):
    #                     for regexStr in self._gen_regex(featureStr):
    #                         appFeatureRegex[app][regexStr] = FRegex(featureStr, regexStr, f)

    def cal_corr(self, trainData, threshold):
        labels = defaultdict(int)
        features = defaultdict(int)
        cooccur = defaultdict(int)
        D = 0
        for tbl, tps in trainData.items():
            D += 1
            for app, agent, host in tps:
                words = SPLITTER.split(agent)
                # print(agent, words)
                for word in set(words):
                    word = word.strip()
                    if len(word) > 1:
                        features[word] += 1
                        cooccur[(app, word)] +=1
                labels[app] += 1

        corr = defaultdict(set)
        unticorr = defaultdict(set)
        for appWord, count in cooccur.items():
            app, word = appWord
            value = ( count * D * 1.0 ) / (labels[app] * features[word])
            #print(app, word, value)
            if value > threshold:
                corr[app].add(word)
            else:
                unticorr[app].add(word)
        return corr, unticorr

    def find_boundary(self, trainData, corr):
        boundary = defaultdict(set)
        for tbl, tps in trainData.items():
            for app, agent, host in tps:
                words = []
                for word in SPLITTER.split(agent):
                    if len(word) > 1:
                        words.append(word)

                words = ['^'] + words + ['$']
                startIndex = -1
                for i, word in enumerate(words):
                    if startIndex == -1 and word in corr[app]:
                        startIndex = i-1
                    if startIndex != -1 and word not in corr[app]:
                        endIndex = i
                        print('#', agent, (words[startIndex], words[endIndex], words[startIndex: endIndex]))
                        boundary[(words[startIndex], words[endIndex])].add(app)
                        startIndex = -1
        return boundary


    def train(self, trainSet, ruleType, ifPersist=True):
        trainData = defaultdict(set)
        potentialHost = defaultdict(set)
        for tbl, pkg in DataSetIter.iter_pkg(trainSet):
            trainData[tbl].add((pkg.app, process_agent(pkg.agent, pkg.app), pkg.host))
            potentialHost[pkg.host].add(pkg.app)
        corr, unticorr = self.cal_corr(trainData, 0.001)
        boundary = self.find_boundary(trainData, corr)
        extractors = {}
        for b, v in boundary.items():
            if len(v) > conf.agent_support:
                extractors[b] = Identifier(b[0], b[1])
                extractors[b].apps = v

        extractors = sorted(extractors.items(), key=lambda x: x[1].weight(), reverse=True)

        '''
        Count regex
        '''
        print 'Len extractors', len(extractors), 'Len agent', len(trainData)
        potentialId  = self.__count(trainData, extractors, unticorr)

        print "Finish Counter"

        # identifierApps, extractors = self._prune(regexApp)
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
        for host, prefix, identifier, suffix, label, support, confidence, ruleType, labelType in sqldao.execute(
                const.sql.SQL_SELECT_AGENT_RULES):
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

            rst[ruleType] = (rstLabel, matchRule)
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