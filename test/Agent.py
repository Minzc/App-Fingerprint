# -*- encoding = utf-8 -*-
from utils import unescape, flatten, load_info_features, load_folder
from collections import defaultdict
import const.consts as consts
import re
import urllib
from const.dataset import DataSetIter as DataSetIter

VALID_FEATURES = {'CFBundleName', 'CFBundleExecutable', 'CFBundleIdentifier',
                  'CFBundleDisplayName', 'CFBundleURLSchemes'}
STRONG_FEATURES = {'CFBundleName', 'CFBundleExecutable', 'CFBundleIdentifier', 'CFBundleDisplayName'}

STOPWORDS = {'iphone', 'app', 'mobile'}

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
    def __init__(self, prefix, suffix):
        self.rule = (prefix, suffix)
        self.prefix = prefix
        self.suffix = suffix
        self.regex = re.compile(prefix + '(.+?)' + suffix)
        self.matched = defaultdict(set)

    def match(self, agent):
        identifier = None
        maxLen = len(agent)
        for m in self.regex.finditer(agent):
            if len(m.group(1)) < maxLen:
                identifier = m.group(1)
                maxLen = len(identifier)
        return identifier

    def add_record(self, app, identifier):
        self.matched[identifier].add(app)


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
        if not featureStr[-1].isalnum():
            featureStr = featureStr[:-1]
        if not featureStr[0].isalnum():
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

    def process_agent(self, agent, app):
        agent = re.sub(r'[a-z]?[0-9]+-[a-z]?[0-9]+-[a-z]?[0-9]+', r'[VERSION]', agent)
        agent = re.sub(r'(/)([0-9]+)([ ;])', r'\1[VERSION]\3', agent)
        agent = re.sub(r'/[0-9][.0-9]+', r'/[VERSION]', agent)
        agent = re.sub(r'([ :])([0-9][.0-9]+)([ ;),])', r'\1[VERSION]\3', agent)
        agent = re.sub(r'([ :])([0-9][_0-9]+)([ ;),])', r'\1[VERSION]\3', agent)
        agent = re.sub(r'(^[0-9a-z]*)(.'+app+r'$)', r'[RANDOM]\2', agent)
        return agent

    def getPrefixNSuffix(self, rule):
        def get_seg(start, stop):
            # print '[get seg]', start, stop, rule[start:stop]
            tmp = rule[start:stop]
            tmp = tmp.replace(u'\0', '[VERSION]').replace(u'\1', '[RANDOM]')
            return re.escape(tmp)

        rule = rule.replace('[VERSION]', u'\0')
        rule = rule.replace('[RANDOM]', u'\1')

        start = rule.find('[IDENTIFIER]')
        end = start + len('[IDENTIFIER]')

        ############# PREFIX #################
        prefixStart = start - 1
        prefixStop = start

        while prefixStart >= 0:
            if rule[prefixStart].isalnum() != True and rule[prefixStart] not in {' ', '.'}:
                break
            else:
                prefixStart -= 1

        prefix = get_seg(prefixStart, prefixStop)
        if prefixStop == 0:
            prefix = r'^' + prefix

        ############# SUFFIX #################
        suffixStart = end
        suffixStop = end


        while suffixStop < len(rule):
            if rule[suffixStop].isalnum() != True and rule[suffixStop] not in {' ', '.'}:
                break
            else:
                suffixStop += 1
        suffix = get_seg(suffixStart, suffixStop + 1)
        if suffixStop == len(rule):
            suffix = suffix+r'$'

        return prefix, suffix


    def train(self, agentTuples):
        generalForms = defaultdict(set)
        for _, appAgent in agentTuples.items():
            for app, agent in appAgent:
                agent = self.process_agent(agent, app)
                for key, value in sorted(self.appFeatures[app].items(), key=lambda x:len(x[1]), reverse=True):
                    if value not in STOPWORDS and value in agent:
                        agent = agent.replace(value, '[IDENTIFIER]')
                        generalForms[agent].add(app)
                        break

        extracers = {}
        for agent, apps in generalForms.items():
            prefix, suffix = self.getPrefixNSuffix(agent)
            extracers[(prefix, suffix)] = Identifier(prefix, suffix)

            # print agent, self.getPrefixNSuffix(agent)

        generalForms = defaultdict(set)
        for _, appAgent in agentTuples.items():
            for app, agent in appAgent:
                # if agent == 'Heat%20Tool/21 CFNetwork/711.4.6 Darwin/14.0.0'.lower():
                #     print '[136]', self.process_agent(agent, app)
                agent = self.process_agent(agent, app)
                for extracer in extracers.values():
                    identifier = extracer.match(agent)
                    if identifier:
                        generalForms[identifier].add(app)
        for identifier, apps in sorted(generalForms.items(), key=lambda x: len(x[1])):
            print identifier, ','.join(apps)


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

    @staticmethod
    def change_raw(rules, trainSet):
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