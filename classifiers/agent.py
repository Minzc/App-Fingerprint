# -*- encoding = utf-8 -*-
from utils import unescape, flatten, load_info_features
from sqldao import SqlDao
from collections import defaultdict
import const.consts as consts
from classifier import AbsClassifer
import re
import urllib



VALID_FEATURES = {'CFBundleName', 'CFBundleExecutable', 'CFBundleIdentifier',
                  'CFBundleDisplayName', 'CFBundleURLSchemes'}
STRONG_FEATURES = {'CFBundleName', 'CFBundleExecutable', 'CFBundleIdentifier', 'CFBundleDisplayName'}


class FRegex:
    def __init__(self, featureStr, regexStr, rawF):
        self.featureStr = featureStr
        self.regexStr = regexStr
        self.rawF = rawF
        self.regexObj = re.compile(regexStr, re.IGNORECASE)
        self.matchRecord = defaultdict(lambda: defaultdict(set))
        self.cover = set()

    def set_match_record(self, host, app, tbls):
        for tbl in tbls:
            self.matchRecord[host][app].add(tbl)

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

    def persist(self, patterns, ruleType):
        """
        Input
        :param patterns : {regex: {app1, app2}}
        :param ruleType : type of rules (App, Company, Category)
        """
        self.clean_db(ruleType, consts.SQL_DELETE_AGENT_RULES)
        sqldao = SqlDao()
        QUERY = consts.SQL_INSERT_AGENT_RULES
        params = []
        for fRegex, apps in filter(lambda item: len(item[1])==1, patterns.iteritems()):
            app = list(apps)[0]
            params.append((app, 1, 1, fRegex.regexObj.pattern, consts.APP_RULE))
        sqldao.executeBatch(QUERY, params)
        sqldao.close()

    def _add_host(self, patterns):
        for fRegex, apps in patterns.iteritems():
            if len(apps) > 1 and fRegex.rawF is not None:
                for host in fRegex.matchRecord:
                    if len(fRegex.matchRecord[host]) == 1:
                        print '[host]', host, fRegex.matchRecord[host], fRegex.featureStr

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
            print 'Type is', f.encode('utf-8'), type(f)

        featureSet.add(f.replace(' ', '%20'))
        featureSet.add(f.replace(' ', '-'))
        featureSet.add(f.replace(' ', '_'))
        featureSet.add(f.replace(' ', ''))
        return featureSet

    def _gen_regex(self, featureStr):
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
                if len(filter(lambda agent: featureStr in agent, agents)) > 0 or app in featureStr:
                    for regexStr in self._gen_regex(featureStr):
                        appFeatureRegex[app][regexStr] = FRegex(featureStr, regexStr, f)

            for agent in filter(lambda x: '/' in x, agents):
                matchStrs = re.findall(r'^[a-zA-Z0-9][0-9a-zA-Z. _\-:&?\'%]+/', agent)
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

    def _prune(self, regexApp):
        """
        :param regexApp: FRegex -> apps
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
                invRegexCover[regexStr].add(str)
        regexApp = sorted(regexApp.items(), key=sortPattern, reverse=True)
        rst = defaultdict(set)
        pruned = defaultdict(set)
        for fRegex, apps in regexApp:
            if len(apps) == 1:
                app = list(apps)[0]
                for regexStr in invRegexCover[app][fRegex.featureStr]:
                    pruned.add(regexStr)
                if fRegex.featureStr not in pruned:
                    rst[fRegex] = apps
            else:
                rst[fRegex] = apps
        return rst





    def _count(self, appFeatureRegex, appAgent):
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

        for predict, regexStr, fRegex in filter(lambda x: x[0] not in appAgent, fAppFeatureRegex):
            regexApp[fRegex].add(predict)

        for app, db in appAgent.items():
            for agntHost, tbls in db.items():
                agent, host = agntHost
                covered = set()
                for predict, regexStr, fRegex in fAppFeatureRegex:
                    if fRegex.featureStr not in agent and fRegex.featureStr not in app:
                        continue
                    if regexStr in covered or fRegex.regexObj.search(agent) or fRegex.regexObj.search(app):
                        regexApp[fRegex].add(app)
                        fRegex.set_match_record(host, app, tbls)
                        for regex in self.regexCover[regexStr]:
                            covered.add(regex)
        return regexApp

    def _infer_from_xml(self, appFeatureRegex, agentTuples):
        for app, features in filter(lambda x: x[0] not in agentTuples, self.appFeatures.items()):
            for f in features.values():
                for featureStr in self._gen_features(f):
                    for regexStr in self._gen_regex(featureStr):
                        appFeatureRegex[app][regexStr] = FRegex(featureStr, regexStr, f)

    @staticmethod
    def _sample_app(agentTuples, sampleRate):
        import random
        agentTuples = {app: agents for app, agents in agentTuples.iteritems() if random.uniform(0, 1) <= sampleRate}
        return agentTuples

    def train(self, trainSet, ruleType):
        agentTuples = defaultdict(set)
        appAgent = defaultdict(lambda : defaultdict(set))
        for tbl, pkgs in trainSet.items():
            for pkg in pkgs:
                label = pkg.label
                agent = pkg.agent
                agentTuples[label].add(agent)
                appAgent[label][(agent, pkg.host)].add(tbl)


        '''
        Sample Apps
        '''
        agentTuples = self._sample_app(agentTuples, self.sampleRate)
        print 'Number of training apps', len(agentTuples)

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
        regexApp = self._count(appFeatureRegex, appAgent)

        print "Finish Counter"

        regexApp = self._prune(regexApp)

        print "Finish Pruning"

        self.persist(regexApp, consts.APP_RULE)

        self._add_host(regexApp)

    def load_rules(self):
        self.rules = {consts.APP_RULE: {}, consts.COMPANY_RULE: {}, consts.CATEGORY_RULE: {}}
        QUERY = consts.SQL_SELECT_AGENT_RULES
        sqldao = SqlDao()
        counter = 0
        for agentF, label, ruleType in sqldao.execute(QUERY):
            counter += 1
            self.rules[ruleType][agentF] = (re.compile(agentF), label)
        print '>>> [Agent Rules#loadRules] total number of rules is', counter, 'Type of Rules', len(self.rules)
        sqldao.close()

    def classify(self, pkg):
        rst = {}
        for ruleType in self.rules:
            longestWord = ''
            rstLabel = None
            for agentF, regxNlabel in self.rules[ruleType].items():
                regex, label = regxNlabel
                if regex.search(pkg.agent) and len(longestWord) < len(agentF):
                    rstLabel = label
                    longestWord = agentF

            rst[ruleType] = consts.Prediction(rstLabel, 1.0, longestWord) if rstLabel else consts.NULLPrediction

            if rstLabel is not None and rstLabel != pkg.app and ruleType == consts.APP_RULE:
                print '>>>[AGENT CLASSIFIER ERROR] agent:', pkg.agent, 'App:', pkg.app, 'Prediction:', rstLabel, \
                    'Longestword:', longestWord
        return rst
