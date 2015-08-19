from utils import longest_common_substring, get_top_domain, url_clean, load_exp_app
from sqldao import SqlDao
from collections import defaultdict
import consts
from classifier import AbsClassifer
import re

test_str = 'androidapp.linecamera/9.0.4 (Linux; U; Android 5.0.2; en-US; Nexus 7 Build/LRX22G)'.lower()

class AgentClassifier(AbsClassifer):
    def clean_agent(self, agent):
      return re.sub('[/].*', '', agent)

    def __init__(self):
      self.agentLabel = defaultdict(set)
      self.rules = defaultdict(dict)
    
    def persist(self, patterns, ruleType):
      self._clean_db(ruleType)
      sqldao = SqlDao()
      QUERY = consts.SQL_INSERT_AGENT_RULES
      params = []
      for ruleType in patterns:
        for agent, label in patterns[ruleType].iteritems():
          params.append((label, 1, 1, agent, ruleType))
      sqldao.executeBatch(QUERY, params)
      sqldao.close()

    def count(self, pkg):
      label = pkg.label
      agent = self.clean_agent(pkg.agent)
      if not agent:
        return
      self.agentLabel[agent].add(label)

    def _clean_db(self, ruleType):
      QUERY = consts.SQL_DELETE_AGENT_RULES
      print QUERY
      sqldao = SqlDao()
      sqldao.execute(QUERY % (ruleType))
      sqldao.close()

    def train(self, records, rule_type):
      for pkg in [pkg for pkgs in records.values() for pkg in pkgs]:
        self.count(pkg)
      ########################
      # Generate Rules
      ########################
      
      print test_str in self.agentLabel

      for agent, labels in self.agentLabel.iteritems():
        if agent == test_str:
          print '#', len(labels)
          print labels
        
        if len(labels) == 1:
          label = labels.pop()

          self.rules[rule_type][agent] = label

          if agent == test_str:
            print 'Rule Type is', rule_type 

      print 'number of rule', len(self.rules[consts.APP_RULE])
      print 'persist'
      self.persist(self.rules, rule_type)
      self.__init__()
      return self

    def load_rules(self):
      self.rules = {consts.APP_RULE:{}, consts.COMPANY_RULE:{}, consts.CATEGORY_RULE:{}}
      QUERY = consts.SQL_SELECT_AGENT_RULES
      sqldao = SqlDao()
      counter = 0
      for agent, label, ruleType in sqldao.execute(QUERY):
        counter += 1
        self.rules[ruleType][agent] = label
      print '>>> [Agent Rules#loadRules] total number of rules is', counter, 'Type of Rules', len(self.rules)
      sqldao.close()

    def classify(self, pkg):
      rst = {}
      for ruleType in self.rules:
        agent = pkg.agent
        label = self.rules[ruleType].get(agent, None)
        rst[ruleType] = (label, 1.0)
        if label != None and label != pkg.app:
            print agent, pkg.app, label
      return rst
