#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import absolute_import, division, print_function, unicode_literals

from collections import defaultdict

import re

import const.sql
import utils
from classifiers.classifier_factory import classifier_factory
from const import consts
from const.dataset import DataSetIter
from sqldao import SqlDao


class Prune:
    def __init__(self, appType):
        self.agentClassifier = classifier_factory([consts.AGENT_CLASSIFIER], appType)[0][1]
        self.queryClassifier = classifier_factory([consts.KV_CLASSIFIER], appType)[0][1]
        self.agentClassifier.load_rules()
        self.queryClassifier.load_rules()

    def prune(self, trainSet):
        utils.clean_rules()
        agentRule = set()
        coverage = defaultdict(int)
        predicts = self.agentClassifier.classify2(trainSet)
        for key, predict in predicts.items():
            coverage[key] += 1
            agentRule.add(predict.evidence)
        agent_persist(agentRule, consts.APP_RULE)

        specificRules = defaultdict(lambda: defaultdict(
            lambda: defaultdict(lambda: defaultdict(lambda: {consts.SCORE: 0, consts.SUPPORT: set()}))))
        for tbl, pkg in DataSetIter.iter_pkg(trainSet):
            rst = self.queryClassifier.p(pkg)[consts.APP_RULE]
            if rst[0] != None and coverage[tbl + '#' + str(pkg.id)] < 1:
                coverage[tbl + '#' + str(pkg.id)] += 1
                key, value, host, label, confidence, rule_type, support = rst[1]
                specificRules[host][key][value][label][consts.SCORE] = confidence
                specificRules[host][key][value][label][consts.SUPPORT] = support
        query_persist(specificRules, consts.APP_RULE)

def agent_persist(appRule, ruleType):
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

def query_persist(specificRules, rule_type):
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
                        support = patterns[host][key][value][label][consts.SUPPORT]
                        params.append((label, support, confidence, host, key, value, ruleType))
    sqldao.executeBatch(QUERY, params)
    sqldao.close()
    print(">>> [KVRules] Total Number of Rules is %s Rule type is %s" % (len(params), rule_type))

