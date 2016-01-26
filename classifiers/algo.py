#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import absolute_import, division, print_function, unicode_literals

import urllib

import const.conf
import const.consts as consts
import re

import const.sql
from classifiers.uri import UriClassifier
from const import conf
from sqldao import SqlDao
from utils import load_xml_features, if_version, flatten, get_label

from collections import defaultdict
from classifiers.classifier import AbsClassifer
from const.dataset import DataSetIter as DataSetIter

DEBUG = False
PATH = '[PATH]:'
HOST = '[HOST]:'


class Path:
    def __init__(self, dbcover, scoreGap):
        self.scoreThreshold = conf.path_scoreT
        self.labelThreshold = conf.path_labelT
        self.name = consts.PATH_MINER
        self.dbcover = dbcover
        self.scoreGap = scoreGap


    def mine_host(self, trainSet, ruleType):
        uriClassifier = UriClassifier(consts.IOS)
        print('[URI] Start Training')
        hostRules, pathRules = uriClassifier.train(trainSet, ruleType, ifPersist=False)
        print('[URI] Finish Training')
        cHosts = {}
        for rule, tbls in hostRules[consts.APP_RULE].items():
            host, _, label = rule
            cHosts[host] = tbls
        print("Total Number of Hosts is", len(cHosts))
        cPath = {}
        for ruleType in pathRules:
            for rule, tbls in hostRules[ruleType].items():
                _, path, label = rule
                cPath[path] = tbls
            print("Total Number of Path is", len(cPath))

        self.cHosts = cHosts
        self.cPath = cPath

    @staticmethod
    def get_f(package):
        host = re.sub('[0-9]+\.', '[0-9]+.', package.rawHost)
        fs = [host] + filter(None, package.path.split('/'))
        tmp = []
        for seg in fs:
            key = PATH + '/'.join(tmp)
            if len(tmp) > 0: key += '/'
            tmp.append(seg)
            value = '/'.join(tmp)
            if if_version(value) == False and len(value) > 1:
                yield (host, key, value)

    def txt_analysis(self, k, v, host, app, tbl, xmlGenRules, xmlSpecificRules):
        pass

    def prune(self, keys):
        """
        key format Rule(secdomain, key, score, labelNum)
        :param keys:
        :return:
        """
        prunedK = {}
        for secdomain, keys in keys.items():
            keys = [key for key in keys if (key.score >= self.scoreThreshold and key.labelNum >= self.labelThreshold) or
                    secdomain in self.cHosts or key.key.split('/')[-1] in self.cPath]
            prunedK[secdomain] = keys
        return prunedK

    def sort(self, genRules, txtRules):
        def compare(genRule):
            ifTxtRule = 1 if (genRule.secdomain, genRule.key) in txtRules else 0
            length = len(genRule.key.split('/'))
            return (ifTxtRule, genRule.score, length)

        sGenRules = sorted(genRules, key=compare, reverse=True)
        return sGenRules

    def gen_txt_rule(self, xmlSpecificRules, specificRules, trackIds):
        return specificRules


def classify_format(package):
    host = package.refer_rawHost if package.refer_rawHost else package.rawHost
    host = re.sub('[0-9]+\.', '[0-9]+.', host)
    path = package.refer_origpath if package.refer_rawHost else package.origPath
    return host, path


class KV:
    def __init__(self, dbcover, scoreGap):
        self.lexicalIds = load_xml_features()
        self.potentialId = defaultdict(set)
        for app, fields in self.lexicalIds.items():
            for _, value in fields:
                self.potentialId[value].add(app)
        self.scoreThreshold = conf.query_scoreT
        self.labelThreshold = conf.query_labelT
        self.dbcover = dbcover
        self.scoreGap = scoreGap
        self.name = consts.KV_MINER

    @staticmethod
    def get_f(package):
        def filter_func(v):
            v = v.strip()
            return if_version(v) == False and len(v) > 1

        host = re.sub('[0-9]+\.', '[0-9]+.', package.rawHost)
        queries = package.queries
        for k, vs in queries.items():
            k = k.replace("\t", "")
            for v in filter(filter_func, vs):
                v = re.sub('[0-9]+x[0-9]+', '', v.strip())
                if '/' in v:
                    v = v.split('/')[0]
                yield (host, k, v)

    def txt_analysis(self, k, v, host, app, tbl, xmlGenRules, xmlSpecificRules):
        """
        Match xml information in training data
        Output
        :return xmlGenRules : (host, key) -> value -> {app}
        :return xmlSpecificRules
        """
        for fieldName in [name for name, value in self.lexicalIds[app] if value == v
        and len(self.potentialId[value]) == 1]:
            xmlGenRules[(host, k)][v][fieldName] += 1
            xmlSpecificRules[(host, k)][v][app].add(tbl)
        for fieldName in [name for name, value in self.lexicalIds[app] if value in v
        and len(self.potentialId[value]) == 1]:
            return consts.Rule(host, k, None, "\b", 1, None)
        return None
            # xmlSpecificRules.add(consts.Rule(host, k, v, '\b', 1, app))


    def prune(self, keys):
        """
        key format Rule(secdomain, key, score, labelNum)
        :param keys:
        :return:
        """
        prunedK = {}
        for secdomain, keys in keys.items():
            keys = [key for key in keys if key.score >= self.scoreThreshold and key.labelNum >= self.labelThreshold]
            prunedK[secdomain] = keys
        return prunedK

    @staticmethod
    def sort(genRules, txtRules):
        def compare(genRule):
            ifTxtRule = 1 if (genRule.secdomain, genRule.key) in txtRules else 0
            return (ifTxtRule, genRule.score, genRule.hostNum, genRule.labelNum)

        sGenRules = sorted(genRules, key=compare, reverse=True)
        return sGenRules

    @staticmethod
    def gen_txt_rule(xmlSpecificRules, specificRules, trackIds):
        """
        :param trackIds:
        :param xmlSpecificRules:
        :param specificRules : specific prune for apps
             host -> key -> value -> label -> { rule.score, support : { tbl, tbl, tbl } }
        """
        for rule, v, app, tbls in flatten(xmlSpecificRules):
            if v not in trackIds and len(re.sub('[0-9]', '', v)) < 2:
                continue
            host, key = rule
            specificRules[host][key][v][app][consts.SCORE] = 1.0
            specificRules[host][key][v][app][consts.SUPPORT] = tbls
        return specificRules

    def mine_host(self, trainSet, ruleType):
        pass


def _generate_keys(keyScore, hostLabelTbl):
    """
    Find interesting ( secdomain, key ) pairs
    Output
    :return generalRules :
        Rule = ( secdomain, key, score, labelNum ) defined in consts/consts.py
        {secdomain : [Rule, Rule, Rule, ... ]}
    """
    generalRules = defaultdict(set)
    hostNum = defaultdict(set)
    for host in keyScore:
        for key in keyScore[host]:
            hostNum[key].add(host)

    for host in keyScore:
        for key in keyScore[host]:
            labelNum = len(keyScore[host][key][consts.LABEL]) / (1.0 * len(hostLabelTbl[host]))
            if host == 'metrics.ally.com':
                print('[algo296]', labelNum, key, len(hostLabelTbl[host]), keyScore[host][key][consts.LABEL])
            score = keyScore[host][key][consts.SCORE]
            generalRules[host].add(consts.QueryKey(host, key, score, labelNum, len(hostNum[key])))
        generalRules[host] = sorted(list(generalRules[host]), key=lambda rule: rule.score, reverse=True)
    return generalRules


def _score(hstKLblValue, vAppCounter, vCategoryCounter, hostLabelTbl):
    """
    Give score to every ( secdomain, key ) pairs
    Input
    :param hstKLblValue :
        Relationships between host, key, value and label(app or company) from training data
        { secdomain : { key : { label : {value} } } }
    :param vAppCounter :
        Relationships between labels(app or company)
        { app : {label} }
    """
    # secdomain -> app -> key -> value -> tbls
    # secdomain -> key -> (label, score)
    appKScore = defaultdict(lambda: defaultdict(lambda: {consts.LABEL: set(), consts.SCORE: 0}))
    categoryKScore = defaultdict(lambda: defaultdict(lambda: {consts.LABEL: set(), consts.SCORE: 0}))

    for host, k, label, v, tbls in flatten(hstKLblValue):
        tbls = len(tbls)
        if tbls > 1:
            numOfValues = len(hstKLblValue[host][k][label])
            numOfLabels = len(hstKLblValue[host][k])
            numOfTbls = len(hostLabelTbl[host][label]) - 1
            if len(vAppCounter[v]) == 1:
                appKScore[host][k][consts.SCORE] += \
                    (tbls - 1) / float(numOfTbls
                                       * numOfValues * numOfValues
                                       * numOfLabels)

            elif len(vCategoryCounter[v]) == 1:
                categoryKScore[host][k][consts.SCORE] += \
                    (tbls - 1) / float(numOfTbls
                                       * numOfValues * numOfValues
                                       * numOfLabels)
        appKScore[host][k][consts.LABEL].add(label)
        categoryKScore[host][k][consts.LABEL].add(label)
        if host == 'metrics.ally.com':
            print('[algo262]',
                  '[Value]', len(hstKLblValue[host][k][label]),
                  '[AllTbls]', len(hostLabelTbl[host][label]),
                  '[Key]', k,
                  '[V]', hstKLblValue[host][k][label],
                  '[B]', (len(vAppCounter[v]) == 1 and if_version(v) == False),
                  '[Tbls]', tbls,
                  '[Labels]', len(hstKLblValue[host][k]),
                  '[Vc]', vAppCounter[v],
                  '[BV]', if_version(v),
                  '[Value]', appKScore[host][k][consts.SCORE])

    print('[algo269APP]', appKScore['metrics.ally.com'])
    print('[algo269CAT]', categoryKScore['metrics.ally.com'])
    return appKScore, categoryKScore


class QueryClassifier(AbsClassifer):
    def __init__(self, appType, minerType):
        self.name = consts.KV_CLASSIFIER
        self.rules = {}
        self.appType = appType

        if minerType == consts.PATH_MINER:
            self.miner = Path(dbcover=1, scoreGap=0.3)
        elif minerType == consts.KV_MINER:
            self.miner = KV(dbcover=3, scoreGap=0.3)

        self.rules = {consts.APP_RULE: defaultdict(lambda: defaultdict(
            lambda: {'score': 0, 'support': 0, 'regexObj': None, 'label': None})),
                      consts.COMPANY_RULE: defaultdict(lambda: defaultdict(
                          lambda: {'score': 0, 'support': 0, 'regexObj': None, 'label': None})),
                      consts.CATEGORY_RULE: defaultdict(lambda: defaultdict(
                          lambda: {'score': 0, 'support': 0, 'regexObj': None, 'label': None}))}

    def _prune_general_rules(self, generalRules, trainData, xmlGenRules):
        """
        1. PK by coverage
        2. Prune by xml prune
        Input
        :param generalRules : {secdomain : [(secdomain, key, score, labelNum), rule, rule]}
        :param trainData : { tbl : [ packet, packet, ... ] }
        :param xmlGenRules : {( host, key) }
        """
        print('[algo217]', generalRules['metrics.ally.com'])
        generalRules = self.miner.prune(generalRules)
        print('[algo219]', generalRules['metrics.ally.com'])

        # Prune by coverage
        for host in generalRules:
            generalRules[host] = self.miner.sort(generalRules[host], xmlGenRules)

        coverage = defaultdict(int)
        prunedGenRules = defaultdict(set)
        for tbl, pkg in DataSetIter.iter_pkg(trainData):
            kv = {}
            for host, key, value in self.miner.get_f(pkg):
                kv[key] = value

            if pkg.host == 'metrics.ally.com':
                print('[algo222]', kv, generalRules['metrics.ally.com'])

            if host in generalRules:
                for rule in generalRules[host]:
                    if rule.key in kv and coverage[tbl + '#' + str(pkg.id)] < self.miner.dbcover:
                        coverage[tbl + '#' + str(pkg.id)] += 1
                        prunedGenRules[host].add(rule)

        # Prune by grouping
        for host, rules in prunedGenRules.items():
            prunedGenRules[host] = sorted(rules, key=lambda x: x[2], reverse=True)
            if host == 'metrics.ally.com':
                print('[algo228]', prunedGenRules[host])
            tmp = set()
            for index, rule in enumerate(prunedGenRules[host]):
                if len(tmp) > 0 and prunedGenRules[host][index - 1].score - rule.score >= self.miner.scoreGap:
                    break
                tmp.add(consts.Rule(host, rule.key, None, "\b", rule.score, None))
            prunedGenRules[host] = tmp
            if host == 'metrics.ally.com':
                print('[algo237]', prunedGenRules[host])
        return prunedGenRules

    def _generate_rules(self, hstKLblValue, generalRules, valueLabelCounter):
        """
        Generate specific prune
        Input
        :param trainData : { tbl : [ packet, packet, packet, ... ] }
        :param generalRules :
            Generated in _generate_keys()
            {secdomain : [Rule, Rule, Rule, ... ]}
        :param valueLabelCounter : Relationships between value and labels

        Output
        :return specificRules : specific prune for apps
            { host : { key : { value : { label : { rule.score, support : { tbl, tbl, tbl } } } } } }
        """
        specificRules = defaultdict(lambda: defaultdict(
            lambda: defaultdict(lambda: defaultdict(lambda: {consts.SCORE: 0, consts.SUPPORT: set()}))))

        for host, rules in generalRules.items():
            for rule in rules:
                for label in hstKLblValue[host][rule.prefix]:
                    for value in hstKLblValue[host][rule.prefix][label]:
                        if len(valueLabelCounter[value]) == 1:
                            specificRules[host][rule.prefix][value][label] = {
                                consts.SCORE   : rule.score,
                                consts.SUPPORT : hstKLblValue[host][rule.prefix][label][value]
                                #consts.SUPPORT : rule.hostNum
                            }
        return specificRules

    @staticmethod
    def _merge_result(appSpecificRules, categorySpecificRules):
        def __create_dic():
            return defaultdict(lambda: defaultdict(
                lambda: defaultdict(lambda: defaultdict(lambda: {consts.SCORE: 0, consts.SUPPORT: set()}))))

        specificRules = {consts.APP_RULE: __create_dic(), consts.CATEGORY_RULE: __create_dic()}
        for host, key, value, app, scoreType, score in flatten(appSpecificRules):
            specificRules[consts.APP_RULE][host][key][value][app][scoreType] = score
        for host, key, value, app, scoreType, score in flatten(categorySpecificRules):
            specificRules[consts.CATEGORY_RULE][host][key][value][app][scoreType] = score
        return specificRules

    # def _infer_from_xml(self, specificRules, xmlGenRules, rmApps):
    #     print 'Start Infering'
    #     xmlFieldValues = defaultdict(lambda: defaultdict(set))
    #     for app in self.xmlFeatures:
    #         for k, v in self.xmlFeatures[app]:
    #             if len(v) != 0 and if_version(v) == False:
    #                 xmlFieldValues[app][k].add(v)
    #     interestedXmlRules = defaultdict(set)
    #     for rule in xmlGenRules:
    #         host, key = rule
    #         if len(specificRules[host][key]) != 0:
    #             for _, fieldName, _ in flatten(xmlGenRules[rule]):
    #                 interestedXmlRules[fieldName].add((host, key, len(specificRules[host][key])))
    #
    #     for fieldName, prune in interestedXmlRules.items():
    #         for app in rmApps:
    #             if len(xmlFieldValues[app][fieldName]) == 1:
    #                 for value in xmlFieldValues[app][fieldName]:
    #                     prune = sorted(prune, key=lambda x: x[2], reverse=True)[:3]
    #                     for rule in prune:
    #                         host, key, score = rule
    #                         specificRules[host][key][value][app][consts.SCORE] = 1.0
    #                         specificRules[host][key][value][app][consts.SUPPORT] = {1, 2, 3, 4}
    #     return specificRules

    def init(self):
        def create_dict():
            return defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: defaultdict(set))))

        compressedDB = {
            consts.APP_RULE: create_dict(),
            consts.CATEGORY_RULE: create_dict()
        }
        valueLabelCounter = {
            consts.APP_RULE: defaultdict(set),
            consts.CATEGORY_RULE: defaultdict(set)
        }
        hostLabelTable = {
            consts.APP_RULE: defaultdict(lambda: defaultdict(set)),
            consts.CATEGORY_RULE: defaultdict(lambda: defaultdict(set))
        }
        lexicalKey = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
        lexicalRules = defaultdict(lambda: defaultdict(lambda: defaultdict(set)))
        return compressedDB, valueLabelCounter, hostLabelTable, lexicalKey, lexicalRules

    def train(self, trainData, rule_type):
        """
        Sample Training Data
        :param rule_type:
        :param trainData:
        """

        def insert_record(host, key, value, label, labelType, tbl):
            compressedDB[labelType][host][key][label][value].add(tbl)
            valueLabelCounter[labelType][value].add(label)
            hostLabelTable[labelType][host][label].add(tbl)

        compressedDB, valueLabelCounter, hostLabelTable, lexicalKey, lexicalRules = self.init()

        self.miner.mine_host(trainData, rule_type)
        trackIds = {}
        baseLine = defaultdict(set)
        for tbl, pkg in DataSetIter.iter_pkg(trainData):
            for host, k, v in self.miner.get_f(pkg):
                insert_record(host, k, v, pkg.app, consts.APP_RULE, tbl)
                insert_record(host, k, v, pkg.category, consts.CATEGORY_RULE, tbl)
                trackIds[pkg.trackId] = pkg.app
                lexicalR = self.miner.txt_analysis(k, v, host, pkg.app, tbl, lexicalKey, lexicalRules)
                if lexicalR:
                    baseLine[lexicalR.host].add(lexicalR)

        ##################
        # Count
        ##################
        appKeyScore, categoryKeyScore = _score(compressedDB[consts.APP_RULE],
                                               valueLabelCounter[consts.APP_RULE],
                                               valueLabelCounter[consts.CATEGORY_RULE],
                                               hostLabelTable[consts.APP_RULE])
        #############################
        # Generate interesting keys
        #############################
        appGeneralRules = _generate_keys(appKeyScore, hostLabelTable[consts.APP_RULE])
        categoryGeneralRules = _generate_keys(categoryKeyScore, hostLabelTable[consts.CATEGORY_RULE])
        #############################
        # Pruning general prune
        #############################
        print(">>>[KV] Before pruning appGeneralRules", len(appGeneralRules))
        appGeneralRules = self._prune_general_rules(appGeneralRules, trainData, lexicalKey)
        categoryGeneralRules = self._prune_general_rules(categoryGeneralRules, trainData, lexicalKey)
        print(">>>[KV] appGeneralRules", len(appGeneralRules))
        print(">>>[KV] categoryGeneralRules", len(categoryGeneralRules))
        #############################
        # Generate specific prune
        #############################
        if const.conf.TestBaseLine:
            appGeneralRules = baseLine

        appSpecificRules = self._generate_rules(compressedDB[consts.APP_RULE], appGeneralRules,
                                                valueLabelCounter[consts.APP_RULE])

        # categorySpecifcRules = self._generate_rules(compressedDB[consts.CATEGORY_RULE], categoryGeneralRules,
        #                                             valueLabelCounter[consts.CATEGORY_RULE])
        categorySpecifcRules = defaultdict(lambda: defaultdict(
            lambda: defaultdict(lambda: defaultdict(lambda: {consts.SCORE: 0, consts.SUPPORT: set()}))))
        # appSpecificRules = self._generate_rules(trainData, appGeneralRules,
        #                                         valueLabelCounter[consts.APP_RULE], consts.APP_RULE)
        #
        # categorySpecifcRules = self._generate_rules(trainData, categoryGeneralRules,
        #                                             valueLabelCounter[consts.CATEGORY_RULE], consts.CATEGORY_RULE)

        # appSpecificRules = self._infer_from_xml(appSpecificRules, lexicalKey, trainData.rmapp)
        appSpecificRules = self.miner.gen_txt_rule(lexicalRules, appSpecificRules, trackIds)
        specificRules = self._merge_result(appSpecificRules, categorySpecifcRules)
        #############################
        # Persist prune
        #############################
        self.persist(specificRules, rule_type)
        return self

    @staticmethod
    def _clean_db(rule_type):
        print('>>> [KVRULES]', const.sql.SQL_DELETE_KV_RULES % rule_type)
        sqldao = SqlDao()
        sqldao.execute(const.sql.SQL_DELETE_KV_RULES % rule_type)
        sqldao.commit()
        sqldao.close()

    def load_rules(self):
        sqldao = SqlDao()
        QUERY = const.sql.SQL_SELECT_KV_RULES
        counter = 0
        for key, value, host, label, confidence, rule_type, support in sqldao.execute(QUERY):
            if len(value.split('\n')) == 1 and ';' not in label:
                if rule_type == consts.APP_RULE:
                    counter += 1
                try:
                    value = urllib.quote(value)
                except:
                    pass

                if PATH in key:
                    if value.count('/') == 0:
                        regexObj = re.compile('.', re.IGNORECASE)
                    else:
                        value = '/'.join(value.split('/')[1:])
                        regexObj = re.compile(r'\b' + re.escape(value) + r'\b', re.IGNORECASE)
                else:
                    regexObj = re.compile(r'\b' + re.escape(key + '=' + value) + r'\b', re.IGNORECASE)

                self.rules[rule_type][host][regexObj][consts.SCORE] = confidence
                self.rules[rule_type][host][regexObj][consts.SUPPORT] = support
                self.rules[rule_type][host][regexObj][consts.LABEL] = label
                self.rules[rule_type][host][regexObj][consts.EVIDENCE] = (key, value, host, label, confidence, rule_type, support)
        print('>>> [KV Rules#Load Rules] total number of prune is', counter)
        sqldao.close()

    def c(self, pkg):
        predictRst = {}
        for ruleType in self.rules:
            fatherScore = -1
            rst = consts.NULLPrediction
            if not pkg.refer_host:
                host, path = classify_format(pkg)
                for regexObj, scores in self.rules[ruleType][host].iteritems():
                    hostRegex = re.compile(host)
                    assert hostRegex.search(pkg.rawHost)
                    if pkg.app == 'com.ally.auto' and pkg.host == 'metrics.ally.com':
                        print('[algo523]', regexObj.search(path), regexObj.pattern, path)

                    if regexObj.search(path):
                        label, support, confidence = scores[consts.LABEL], scores[consts.SUPPORT], scores[consts.SCORE]
                        if confidence > rst.score or (confidence == rst.score and support > fatherScore):
                            fatherScore = support
                            evidence = (host, regexObj.pattern)
                            rst = consts.Prediction(label, confidence, evidence)
            predictRst[ruleType] = rst
            if rst != consts.NULLPrediction and rst.label != get_label(pkg, ruleType):
                print('[WRONG]', rst, pkg.app, pkg.category, ruleType)
                print('=' * 10)

        return predictRst

    def persist(self, specificRules, rule_type):
        """
        :param rule_type:
        :param specificRules: specific prune for apps
            ruleType -> host -> key -> value -> label -> { rule.score, support : { tbl, tbl, tbl } }
        """
        QUERY = const.sql.SQL_INSERT_KV_RULES
        sqldao = SqlDao()
        # Param prune
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
        print(">>> [KVRules] Total Number of Rules is %s Rule type is %s" % (len(params), rule_type))

    def p(self, pkg):
        predictRst = {}
        for ruleType in self.rules:
            fatherScore = -1
            predictRst[ruleType] = consts.NULLPrediction
            if not pkg.refer_host:
                rst = consts.NULLPrediction
                host, path = classify_format(pkg)
                for regexObj, scores in self.rules[ruleType][host].iteritems():
                    hostRegex = re.compile(host)
                    assert hostRegex.search(pkg.rawHost)
                    label, support, confidence = scores[consts.LABEL], scores[consts.SUPPORT], scores[consts.SCORE]
                    if regexObj.search(path):
                        if confidence > rst.score or (confidence == rst.score and support > fatherScore):
                            fatherScore = support
                            rst = consts.Prediction(label, confidence, scores[consts.EVIDENCE])
                predictRst[ruleType] = rst
        return predictRst
