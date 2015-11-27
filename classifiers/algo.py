import const.consts as consts
import re
from sqldao import SqlDao
from utils import load_xml_features, if_version, flatten
from collections import defaultdict
from classifier import AbsClassifer
from const.dataset import DataSetIter as DataSetIter

DEBUG = False

class KVClassifier(AbsClassifer):
    def __init__(self, appType, inferFrmData=True, sampleRate=1):
        def __create_dict():
            return defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: defaultdict(set))))

        self.name = consts.KV_CLASSIFIER
        self.compressedDB = {consts.APP_RULE: __create_dict(), consts.COMPANY_RULE: __create_dict()}
        self.valueLabelCounter = {consts.APP_RULE: defaultdict(set), consts.COMPANY_RULE: defaultdict(set)}
        self.rules = {}
        self.appType = appType
        self.xmlFeatures = load_xml_features()
        self.inferFrmData = inferFrmData
        self.sampleRate = sampleRate
        self.rules = {consts.APP_RULE: defaultdict(lambda: defaultdict(
                          lambda: {'score': 0, 'support': 0, 'regexObj': None, 'label': None})),
                      consts.COMPANY_RULE: defaultdict(lambda: defaultdict(
                          lambda: {'score': 0, 'support': 0, 'regexObj': None, 'label': None})),
                      consts.CATEGORY_RULE: defaultdict(lambda: defaultdict(
                          lambda: {'score': 0, 'support': 0, 'regexObj': None, 'label': None}))}

    @staticmethod
    def _prune_general_rules(generalRules, trainData, xmlGenRules):
        """
        1. PK by coverage
        2. Prune by xml rules
        Input
        :param generalRules : {secdomain : [(secdomain, key, score, labelNum), rule, rule]}
        :param trainData : { tbl : [ packet, packet, ... ] }
        :param xmlGenRules : {( host, key) }
        """
        ruleCoverage = defaultdict(lambda: defaultdict(set))
        ruleScores = {}
        ruleLabelNum = {}
        for tbl, pkg, key, value in DataSetIter.iter_kv(trainData):
            for rule in [r for r in generalRules[pkg.secdomain] if r.key == key]:
                ruleCoverage[pkg.host][rule.key].add(tbl + '#' + str(pkg.id))
                ruleScores[(pkg.host, rule.key)] = rule.score
                ruleLabelNum[(pkg.host, rule.key)] = rule.labelNum

        PKG_IDS = 1
        prunedGenRules = defaultdict(list)
        for host, keyNcoveredIds in ruleCoverage.iteritems():
            keyNcoveredIds = sorted(keyNcoveredIds.items(), key=lambda keyNid: len(keyNid[PKG_IDS]))
            for i in range(len(keyNcoveredIds)):
                ifKeepRule = (True, None)
                iKey, iCoveredIds = keyNcoveredIds[i]
                if (host, iKey) not in xmlGenRules and ruleScores[(host, iKey)] < 1:
                    ifKeepRule = (False, None, '3')
                ''' Prune by coverage '''
                for j in range(i + 1, len(keyNcoveredIds)):
                    jKey, jCoveredIds = keyNcoveredIds[j]
                    if ruleScores[(host, iKey)] < ruleScores[(host, jKey)]:
                        if iCoveredIds.issubset(jCoveredIds) and (host, iKey) not in xmlGenRules:
                            ifKeepRule = (False, jKey, '1')
                if iKey == 'devapp':
                    print ifKeepRule, host, ruleScores[(host, iKey)]
                ''' Prune by believing xml rules'''
                # for j in range(1, len(keyNcoveredIds)):
                #     jKey, jCoveredIds = keyNcoveredIds[j]
                #     if (host, jKey) in xmlGenRules and (host, iKey) not in xmlGenRules:
                #         ifKeepRule = (False, jKey, '2')

                if ifKeepRule[0]:
                    rule = consts.Rule(host, iKey, ruleScores[(host, iKey)], ruleLabelNum[(host, iKey)])
                    prunedGenRules[host].append(rule)

        for host, rules in prunedGenRules.items():
            prunedGenRules[host] = sorted(rules, key=lambda x: x[2], reverse=True)
            tmp = []
            counter = 0
            for index, rule in enumerate(prunedGenRules[host]):
                if counter == 1 or prunedGenRules[host][index][2] - rule[2] >= 1:
                    break
                if rule[2] < 2:
                    counter += 1
                tmp.append(rule)
            prunedGenRules[host] = tmp
        return prunedGenRules

    @staticmethod
    def _count(featureTbl, valueLabelCounter):
        """
        Give score to every ( secdomain, key ) pairs
        Input
        :param featureTbl :
            Relationships between host, key, value and label(app or company) from training data
            { secdomain : { key : { label : {value} } } }
        :param valueLabelCounter :
            Relationships between labels(app or company)
            { app : {label} }
        """
        # secdomain -> app -> key -> value -> tbls
        # secdomain -> key -> (label, score)
        keyScore = defaultdict(lambda: defaultdict(lambda: {consts.LABEL: set(), consts.SCORE: 0}))
        for secdomain, k, label, v, tbls in flatten(featureTbl):
            cleanedK = k.replace("\t", "")
            if len(valueLabelCounter[v]) == 1 and if_version(v) == False:
                numOfValues = len(featureTbl[secdomain][k][label])
                keyScore[secdomain][cleanedK][consts.SCORE] += \
                    (len(tbls) - 1) / float(numOfValues * numOfValues * len(featureTbl[secdomain][k]))
                keyScore[secdomain][cleanedK][consts.LABEL].add(label)

        return keyScore

    @staticmethod
    def _generate_keys(keyScore):
        """
        Find interesting ( secdomain, key ) pairs
        Output
        :return generalRules :
            Rule = ( secdomain, key, score, labelNum ) defined in consts/consts.py
            {secdomain : [Rule, Rule, Rule, ... ]}
        """
        Rule = consts.Rule
        generalRules = defaultdict(list)
        for secdomain in keyScore:
            for key in keyScore[secdomain]:
                labelNum = len(keyScore[secdomain][key][consts.LABEL])
                score = keyScore[secdomain][key][consts.SCORE]
                if labelNum == 1 or score <= 0.5:
                    continue
                generalRules[secdomain].append(Rule(secdomain, key, score, labelNum))
        for secdomain in generalRules:
            generalRules[secdomain] = sorted(generalRules[secdomain], key=lambda rule: rule.score, reverse=True)
        return generalRules

    @staticmethod
    def _generate_rules(trainData, generalRules, valueLabelCounter, ruleType):
        """
        Generate specific rules
        Input
        :param trainData : { tbl : [ packet, packet, packet, ... ] }
        :param generalRules :
            Generated in _generate_keys()
            {secdomain : [Rule, Rule, Rule, ... ]}
        :param valueLabelCounter : Relationships between value and labels

        Output
        :return specificRules : specific rules for apps
            { host : { key : { value : { label : { rule.score, support : { tbl, tbl, tbl } } } } } }
        """
        specificRules = defaultdict(lambda: defaultdict(
            lambda: defaultdict(lambda: defaultdict(lambda: {consts.SCORE: 0, consts.SUPPORT: set()}))))

        for tbl, pkg, key, value in DataSetIter.iter_kv(trainData):
            for rule in [r for r in generalRules[pkg.host] if r.key == key]:
                value = value.strip()
                if len(valueLabelCounter[value]) == 1 and len(value) != 1:
                    label = pkg.app if ruleType == consts.APP_RULE else pkg.company
                    specificRules[pkg.host][key][value][label][consts.SCORE] = rule.score
                    specificRules[pkg.host][key][value][label][consts.SUPPORT].add(tbl)

        return specificRules

    @staticmethod
    def _merge_result(appSpecificRules):
        def __create_dic():
            return defaultdict(lambda: defaultdict(
                lambda: defaultdict(lambda: defaultdict(lambda: {consts.SCORE: 0, consts.SUPPORT: set()}))))

        specificRules = {consts.APP_RULE: __create_dic(), consts.COMPANY_RULE: __create_dic()}
        for host, key, value, app, scoreType, score in flatten(appSpecificRules):
            specificRules[consts.APP_RULE][host][key][value][app][scoreType] = score
            # specificRules[consts.COMPANY_RULE][host][key][value][self.appCompanyRelation[app]][scoreType] = score
        # for host in companySpecificRules:
        #   for key in companySpecificRules[host]:
        #     for value in companySpecificRules[host][key]:
        #       for company, scores in companySpecificRules[host][key][value].iteritems():
        #         if len(specificRules[consts.COMPANY_RULE][host][key][value]) == 0:
        #           specificRules[consts.COMPANY_RULE][host][key][value][company] = scores
        #           specificRules[consts.APP_RULE][host][key][value][';'.join(self.companyAppRelation[company])] = scores
        return specificRules

    def __compare(self, trainData, specificRules, hostSecdomain, appKeyScore):
        """
        Compare xml rules and learned rules
        :param trainData
        :param specificRules specific rules of app
        """
        xmlValueField = defaultdict(lambda: defaultdict(set))
        xmlFieldValues = defaultdict(lambda: defaultdict(set))
        for app in self.xmlFeatures:
            for k, v in self.xmlFeatures[app]:
                xmlFieldValues[app][k].add(v)
                xmlValueField[app][v].add(k)
        tmpRules = set()
        for tbl, pkg, k, v in DataSetIter.iter_kv(trainData):
            app = pkg.app
            if not if_version(v) and v in xmlValueField[app] and len(self.valueLabelCounter[consts.APP_RULE][v]) == 1:
                tmpRules.add((pkg.host, k, v, pkg.app))
        for host, key, value, app in tmpRules:
            if app not in specificRules[consts.APP_RULE][host][key][value]:
                secdomain = hostSecdomain[host]
                labelNum = len(appKeyScore[secdomain][key][consts.LABEL])
                score = appKeyScore[secdomain][key][consts.SCORE]
                print '[Host] {0:s} [key] {1:s} [Value] {2:s} [App] {3:s} [Num] {4:d} [Score] {5:f}' \
                    .format(host, key, value, app, labelNum, score)

    def _gen_xml_rules(self, trainData):
        """
        Match xml information in training data
        Output
        :return xmlGenRules : (host, key) -> value -> {app}
        :return xmlSpecificRules
        """
        xmlGenRules = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
        xmlSpecificRules = defaultdict(lambda: defaultdict(lambda: defaultdict(set)))
        hostSecdomain = {}
        for tbl, pkg, k, v in DataSetIter.iter_kv(trainData):
            self.valueLabelCounter[consts.APP_RULE][v].add(pkg.app)
            hostSecdomain[pkg.host] = pkg.secdomain
            if if_version(v) == False and len(self.valueLabelCounter[consts.APP_RULE][v]) == 1:
                for fieldName in [name for name, value in self.xmlFeatures[pkg.app] if value == v]:
                    xmlGenRules[(pkg.secdomain, k)][v][fieldName] += 1
                    xmlGenRules[(pkg.host, k)][v][fieldName] += 1
                    xmlSpecificRules[(pkg.host, k)][v][pkg.app].add(tbl)

        return xmlGenRules, xmlSpecificRules, hostSecdomain

    @staticmethod
    def gen_specific_rules_xml(xmlSpecificRules, specificRules, trackIds):
        """
        :param trackIds:
        :param xmlSpecificRules:
        :param specificRules : specific rules for apps
             host -> key -> value -> label -> { rule.score, support : { tbl, tbl, tbl } }
        """
        for rule, v, app, tbls in flatten(xmlSpecificRules):
            if v not in trackIds and len(re.sub('[0-9]', '', v)) < 2:
                continue
            host, key = rule
            specificRules[host][key][v][app][consts.SCORE] = 1.0
            specificRules[host][key][v][app][consts.SUPPORT] = tbls
        return specificRules

    def _infer_from_xml(self, specificRules, xmlGenRules, rmApps):
        print 'Start Infering'
        xmlFieldValues = defaultdict(lambda: defaultdict(set))
        for app in self.xmlFeatures:
            for k, v in self.xmlFeatures[app]:
                if len(v) != 0 and if_version(v) == False:
                    xmlFieldValues[app][k].add(v)
        interestedXmlRules = defaultdict(set)
        for rule in xmlGenRules:
            host, key = rule
            if len(specificRules[host][key]) != 0:
                for _, fieldName, _ in flatten(xmlGenRules[rule]):
                    interestedXmlRules[fieldName].add((host, key, len(specificRules[host][key])))

        for fieldName, rules in interestedXmlRules.items():
            for app in rmApps:
                if len(xmlFieldValues[app][fieldName]) == 1:
                    for value in xmlFieldValues[app][fieldName]:
                        rules = sorted(rules, key=lambda x: x[2], reverse=True)[:3]
                        for rule in rules:
                            host, key, score = rule
                            print 'Infer One Rule', host, key, value.encode('utf-8'), app
                            specificRules[host][key][value][app][consts.SCORE] = 1.0
                            specificRules[host][key][value][app][consts.SUPPORT] = {1, 2, 3, 4}
        return specificRules

    def train(self, trainData, rule_type):
        """
        Sample Training Data
        :param rule_type:
        :param trainData:
        """

        trackIds = {}
        for tbl, pkg, k, v in DataSetIter.iter_kv(trainData):
            self.compressedDB[consts.APP_RULE][pkg.secdomain][k][pkg.label][v].add(tbl)
            self.compressedDB[consts.COMPANY_RULE][pkg.secdomain][k][pkg.company][v].add(tbl)
            self.valueLabelCounter[consts.APP_RULE][v].add(pkg.label)
            self.valueLabelCounter[consts.COMPANY_RULE][v].add(pkg.company)
            trackIds[pkg.trackId] = pkg.app

        xmlGenRules, xmlSpecificRules, hostSecdomain = self._gen_xml_rules(trainData)
        ##################
        # Count
        ##################
        appKeyScore = self._count(self.compressedDB[consts.APP_RULE], self.valueLabelCounter[consts.APP_RULE])
        companyKeyScore = self._count(self.compressedDB[consts.COMPANY_RULE],
                                      self.valueLabelCounter[consts.COMPANY_RULE])
        #############################
        # Generate interesting keys
        #############################
        appGeneralRules = self._generate_keys(appKeyScore)
        companyGeneralRules = self._generate_keys(companyKeyScore)
        #############################
        # Pruning general rules
        #############################
        appGeneralRules = self._prune_general_rules(appGeneralRules, trainData, xmlGenRules)
        companyGeneralRules = self._prune_general_rules(companyGeneralRules, trainData, xmlGenRules)
        print ">>>[KV] appGeneralRules", len(appGeneralRules)
        print ">>>[KV] companyGeneralRules", len(companyGeneralRules)
        #############################
        # Generate specific rules
        #############################
        appSpecificRules = self._generate_rules(trainData, appGeneralRules, self.valueLabelCounter[consts.APP_RULE],
                                                consts.APP_RULE)
        print 'Infer from data', self.inferFrmData

        if self.inferFrmData:
            appSpecificRules = self._infer_from_xml(appSpecificRules, xmlGenRules, trainData.rmapp)
        appSpecificRules = self.gen_specific_rules_xml(xmlSpecificRules, appSpecificRules, trackIds)
        companySpecificRules = self._generate_rules(trainData, companyGeneralRules,
                                                    self.valueLabelCounter[consts.COMPANY_RULE], consts.COMPANY_RULE)
        specificRules = self._merge_result(appSpecificRules)
        specificRules = self.change_raw(specificRules, trainData)
        #############################
        # Persist rules
        #############################
        self.persist(specificRules, rule_type)
        self.__init__(self.appType)
        return self

    @staticmethod
    def _clean_db(rule_type):
        print '>>> [KVRULES]', consts.SQL_DELETE_KV_RULES % rule_type
        sqldao = SqlDao()
        sqldao.execute(consts.SQL_DELETE_KV_RULES % rule_type)
        sqldao.commit()
        sqldao.close()

    def load_rules(self):
        sqldao = SqlDao()

        QUERY = consts.SQL_SELECT_KV_RULES
        counter = 0
        for key, value, host, label, confidence, rule_type, support in sqldao.execute(QUERY):
            if len(value.split('\n')) == 1 and ';' not in label:
                if rule_type == consts.APP_RULE:
                    counter += 1

                regexObj = re.compile(re.escape(key + '=' + value))
                self.rules[rule_type][host][regexObj][consts.SCORE] = confidence
                self.rules[rule_type][host][regexObj][consts.SUPPORT] = support
                self.rules[rule_type][host][regexObj][consts.REGEX_OBJ] = label
        print '>>> [KV Rules#Load Rules] total number of rules is', counter
        sqldao.close()

    def c(self, pkg):
        predictRst = {}
        for ruleType in self.rules:
            fatherScore = -1
            rst = consts.NULLPrediction
            host = pkg.refer_rawHost if pkg.refer_rawHost else pkg.rawHost
            for regexObj, scores in self.rules[ruleType][host].iteritems():
                path = pkg.refer_origpath if pkg.refer_rawHost else pkg.origPath
                # if 'ads.mopub.com' in host:
                #     print '[HOST IN]', host, path
                #     print '[PATTERN]', regexObj.pattern
                #     print regexObj.search(path)
                if regexObj.search(path):
                    label, support, confidence = scores['label'], scores[consts.SUPPORT] ,scores[consts.SCORE]
                    print support, confidence, rst.score
                    print support > rst.score
                    if support > rst.score or (support == rst.score and confidence > fatherScore):
                        fatherScore = confidence
                        evidence = (host, regexObj.pattern)
                        rst = consts.Prediction(label, support, evidence)
                        print '[HIT]', rst

            predictRst[ruleType] = rst
        print predictRst[consts.APP_RULE]
        return predictRst

    @staticmethod
    def change_raw(specificRules, trainData):
        tmpSpecificRules = {}
        for ruleType, patterns in specificRules.iteritems():
            tmpRules = {}
            for tbl, pkg, key, value in DataSetIter.iter_kv(trainData):
                if pkg.label in patterns[pkg.host][key][value]:
                    tmpRules[pkg.rawHost] = patterns[pkg.host]
            tmpSpecificRules[ruleType] = tmpRules
        return tmpSpecificRules

    def persist(self, specificRules, rule_type):
        """
        :param rule_type:
        :param specificRules: specific rules for apps
            ruleType -> host -> key -> value -> label -> { rule.score, support : { tbl, tbl, tbl } }
        """
        self._clean_db(rule_type)
        QUERY = consts.SQL_INSERT_KV_RULES
        sqldao = SqlDao()
        # Param rules
        params = []
        for ruleType, patterns in specificRules.iteritems():
            for host in patterns:
                for key in patterns[host]:
                    for value in patterns[host][key]:
                        for label in patterns[host][key][value]:
                            confidence = patterns[host][key][value][label][consts.SCORE]
                            support = len(patterns[host][key][value][label][consts.SUPPORT])
                            params.append((label, support, confidence, host, key, value, ruleType))
        sqldao.executeBatch(QUERY, params)
        sqldao.close()
        print ">>> [KVRules] Total Number of Rules is %s Rule type is %s" % (len(params), rule_type)
