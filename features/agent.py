# -*- encoding = utf-8 -*-
from collections import defaultdict

from const import consts
from sqldao import SqlDao
import re

HOST = '[HOST]:'
AGENT = '[AGENT]:'
PATH = '[PATH]:'

def load_agent():
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

class AgentEncoder:
    def __init__(self):
        self.__agentF = load_agent()

    def get_feature(self, package, prefix=True):
        agent = self.get_agent(package)
        pathSegs = package.path.split('/')
        host = package.host
        if prefix:
            agent = AGENT + agent
            pathSegs = map(lambda seg: PATH + seg, pathSegs)
            host = HOST + host
        return [agent] + pathSegs + [host]

    def get_agent(self, package):
        app = package.app
        agent = package.agent
        for regex in self.__agentF[app]:
            if regex.search(agent):
                return regex.pattern
        return None

    def change2Rule(self, strList):
        agent = None
        pathSeg = None
        host = None
        for str in strList:
            if HOST in str:
                host = str.replace(HOST, '')
            if AGENT in str:
                agent = str.replace(AGENT, '')
            if PATH in str:
                pathSeg = str.replace(PATH, '')

        return (pathSeg, agent, host)

    def changeRule2Para(self, agentRules):
        params = []
        for rule in agentRules:
            agent, path, host, classlabel, confidence, support = rule
            params.append((classlabel, path, agent, confidence, support, host, consts.APP_RULE))
        return params