# -*- encoding = utf-8 -*-
from collections import defaultdict

from const import consts
from sqldao import SqlDao

HOST = '[HOST]:'
AGENT = '[AGENT]:'
class AgentEncoder:
    def __init__(self):
        self.rules = self.load_agent()

    def load_agent(self):
        import re
        rules = defaultdict(set)
        QUERY = consts.SQL_SELECT_AGENT_RULES
        sqldao = SqlDao()
        counter = 0
        for host, agentF, label, ruleType in sqldao.execute(QUERY):
            counter += 1
            rules[label].add(re.compile(agentF))
        print '>>> [Agent Rules#loadRules] total number of rules is', counter, 'Type of Rules', len(rules)
        sqldao.close()
        return rules

    def get_feature(self, package):
        app = package.app
        agent = package.agent
        for regex in self.rules[app]:
            if regex.search(agent):
                return [AGENT + regex.pattern, HOST + package.host]
        return []

    def change2Rule(self, strList):
        agent = None
        host = None
        for str in strList:
            if HOST in str:
                host = str.replace(HOST, '')
            if AGENT in str:
                agent = str.replace(AGENT, '')
        return (agent, host)

    def changeRule2Para(self, agentRules):
        # agentRules[agent][host] = classlabel
        params = []
        for rule in agentRules:
            agent, path, host, classlabel, confidence, support = rule
            params.append((classlabel, path, agent, confidence, support, host, consts.APP_RULE))
        return params