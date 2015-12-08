# -*- encoding = utf-8 -*-
from const import consts
from sqldao import SqlDao
from utils import unescape, flatten, load_info_features, load_folder, process_agent
from collections import defaultdict
#import const.consts as consts
import re
import urllib
#from const.dataset import DataSetIter as DataSetIter

VALID_FEATURES = {'CFBundleName', 'CFBundleExecutable', 'CFBundleIdentifier',
                  'CFBundleDisplayName', 'CFBundleURLSchemes'}
STRONG_FEATURES = {'CFBundleName', 'CFBundleExecutable', 'CFBundleIdentifier', 'CFBundleDisplayName'}

STOPWORDS = {'iphone', 'app', 'mobile' ,'cfnetwork'}


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
    # def __init__(self, prefix, suffix):
    #     self.rule = (prefix, suffix)
    #     self.prefix = prefix
    #     self.suffix = suffix
    #     self.regex = re.compile(prefix + '([^/]+?)' + suffix)
    #     self.matched = defaultdict(set)
    def __init__(self, rule):
            start = rule.find(consts.IDENTIFIER)
            end = start + len(consts.IDENTIFIER)
            prefix = r'^' + re.escape(rule[:start])
            suffix = re.escape(rule[end:])+'$'
            self.ruleStr = rule
            self.prefix = re.compile(prefix)
            self.suffix = re.compile(suffix)
            self.matched = defaultdict(set)

    def match(self, agent):
        if self.prefix.search(agent) and self.suffix.search(agent):
            agent = self.prefix.sub('', agent)
            agent = self.suffix.sub('', agent)
        else:
            agent = None
        return agent

    def add_identifier(self, app, identifier):
        self.matched[app].add(identifier)

    def weight(self):
        return len(self.matched)

    def check(self, identifier):
        for app, identifiers in self.matched.items():
            if identifier in identifiers:
                return True
        return False
    def gen(self, identifier):
        return self.prefix.pattern + re.escape(identifier) + self.suffix.pattern
    # def match(self, agent):
    #     identifier = None
    #     maxLen = len(agent)
    #     for m in self.regex.finditer(agent):
    #         if len(m.group(1)) < maxLen:
    #             identifier = m.group(1)
    #             maxLen = len(identifier)
    #     return identifier
class AgentClassifier():
    def __init__(self, inferFrmData=True, sampleRate=1):
        self.rules = defaultdict(dict)
        self.appFeatures = load_info_features(self._parse_xml)

        self.valueApp = defaultdict(set)
        for app, features in self.appFeatures.items():
            for f in features.values():
                self.valueApp[f].add(app)

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

                value = unescape(value.lower()).strip()
                if value.lower() not in STOPWORDS:
                    features[key] = value
        return features

    def persist(self, appRule, companyRule, hostAgent, ruleType):
        """
        Input
        :param companyRule:
        :param appRule : {regex: {app1, app2}}
        :param ruleType : type of rules (App, Company, Category)
        :param hostAgent: (host, regex) -> label
        """
        # self.clean_db(ruleType, consts.SQL_DELETE_AGENT_RULES)
        # sqldao = SqlDao()

        rules = {}
        for regexStr, app in appRule.iteritems():
            regexStr = regexStr.replace(re.escape(consts.VERSION), r'\b[a-z0-9-.]+\b')
            regexObj = re.compile(regexStr, re.IGNORECASE)
            rules[regexObj] = app
        return rules



    @staticmethod
    def _company(patterns):
        companyRule = {}
        for fRegex, apps in patterns.iteritems():
            if len(apps) > 1 and fRegex.rawF is not None and len(fRegex.matchCompany) == 1:
                companyRule[fRegex] = list(fRegex.matchCompany)[0]
        return companyRule

    @staticmethod
    def _app(identifierApps, extractors, hostCategory):
        appRules = {}
        hostAgentRule = {}

        check = set()
        for _, extractor in extractors:
            for app, identifiers in extractor.matched.items():
                for identifier in identifiers:
                    if len(identifierApps[identifier]) == 1:
                        appRules[extractor.gen(identifier)] = app
                    elif len(identifierApps[identifier]) < 10:
                        check.add(identifier)
        for identifier in check:
            print '[CHECK]',identifier, identifierApps[identifier]

        # for fRegex, apps in patterns.iteritems():
        #     if len(apps) > 1 and fRegex.rawF is not None and len(fRegex.matchCategory) == 1:
        #         for host in fRegex.matchRecord:
        #             if len(fRegex.matchRecord[host]) == 1 and len(hostCategory[host]) == 1:
        #                 hostAgentRule[(host, fRegex.regexObj.pattern)] = list(fRegex.matchRecord[host])[0]
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
        extractors = {}
        for appAgent in agentTuples.values():
            for app, agent in appAgent:
                for key, v in sorted(self.appFeatures[app].items(), key=sortFunc, reverse=True):
                    ifMatch = False
                    for value in self._gen_features(v):
                        if value not in STOPWORDS and value in agent:
                            tmp = agent.replace(value, consts.IDENTIFIER, 1)
                            # prefix, suffix = self.getPrefixNSuffix(agent)
                            if tmp not in extractors: extractors[tmp] = Identifier(tmp)
                            extractors[tmp].add_identifier(app, value)
                            if value == 'bingo%20bash':
                                print '[SIZE0]', agent
                            ifMatch = True
                            break
                    if ifMatch == True:
                        break
        return extractors

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
            for string in regexStrs:
                invRegexCover[string].add(regexStr)
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

    def _count(self, agentTuples, extractors):
        """
        Count regex
        :param appAgent: app -> (host, agent) -> tbls
        """

        identifierApps = defaultdict(set)
        notDisAgent = set()
        covered = set()
        for _, appAgent in agentTuples.items():
            for app, agent in appAgent:
                ifMatch = False
                for key, extractor in filter(lambda x: x[1].weight() > 10, extractors):
                    identifier = extractor.match(agent)

                    if identifier == 'assistantservices':
                        print agent, app

                    if identifier:
                        ifMatch = True
                        if app == 'com.speaktoit.assistant':
                            print '[IDENTIFIER]', identifier
                        extractor.add_identifier(app, identifier)
                        identifierApps[identifier].add(app)
                        covered.add(app)
                        break
                if ifMatch == False:
                    for key, extractor in filter(lambda x: x[1].weight() <= 10, extractors):
                        identifier = extractor.match(agent)
                        if identifier and extractor.check(identifier):
                            ifMatch = True
                            identifierApps[identifier].add(app)
                            break
                if ifMatch == False:
                    notDisAgent.add(agent)
        return identifierApps, extractors

    def _infer_from_xml(self, appFeatureRegex, agentTuples):
        for app, features in filter(lambda x: x[0] not in agentTuples, self.appFeatures.items()):
            for f in features.values():
                if len(self.valueApp[f]) == 1 and f not in STOPWORDS:
                    for featureStr in self._gen_features(f):
                        for regexStr in self._gen_regex(featureStr):
                            appFeatureRegex[app][regexStr] = FRegex(featureStr, regexStr, f)

    def train(self, agentTuples):
        newAgentTuples = defaultdict(set)
        for tbl, tuples in agentTuples.items():
            for app, agent in tuples:
                newAgentTuples[tbl].add((app, process_agent(agent, app)))

        '''
        Compose regular expression
        '''
        extractors = self.__compose_idextractor(newAgentTuples)
        extractors = sorted(extractors.items(), key=lambda x: x[1].weight(), reverse=True)

        print 'Infer From Data Is', self.inferFrmData
        # if self.inferFrmData:
        #     self._infer_from_xml(appFeatureRegex, agentTuples)

        '''
        Count regex
        '''
        identifierApps, extractors = self._count(newAgentTuples, extractors)

        print "Finish Counter"

        hostCategory = defaultdict(set)
        # identifierApps, extractors = self._prune(regexApp)
        appRule, hostAgent = self._app(identifierApps, extractors, hostCategory)
        #companyRule = self._company(regexApp)


        print "Finish Pruning"

        # hostAgent = self._add_host(regexApp, hostCategory)
        # hostAgent = self.change_raw(hostAgent, trainSet)

        rules = self.persist(appRule, {}, hostAgent, consts.APP_RULE)

        identifierApps = set()
        for _, appAgent in agentTuples.items():
            for app, agent in appAgent:
                for regexObj, pApp in rules.items():
                    if regexObj.match(agent):
                        identifierApps.add(app)
                        print 'MATCH [Predict]', pApp, '[AGENT]', agent, '[RULE]', regexObj.pattern
                        if app != pApp:
                            print 'ERROR [Predict]', pApp, '[APP]', app, '[RULE]', regexObj.pattern, '[AGENT]', agent

        print '[FINISH]', len(identifierApps)


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

def load_files():
    fileContents = load_folder('db')
    data = defaultdict(set)
    for fileName, content in fileContents.items():
        for i in range(1, len(content)):
            app, agent = content[i].strip().split(',', 1)
            app = app[1:-1].lower().decode('ascii')
            agent = agent[1:-1].lower().decode('ascii')
            data[fileName].add((app, agent))
    return data

if __name__ == '__main__':
    appAgents = load_files()
    classifier = AgentClassifier()
    classifier.train(appAgents)