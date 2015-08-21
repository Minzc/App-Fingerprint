from utils import longest_common_substring, backward_maxmatch
from sqldao import SqlDao
from collections import defaultdict
import consts
from classifier import AbsClassifer
import re

test_str = 'Antler%20Insanity/1.75 CFNetwork/711.4.6 Darwin/14.0.0'.lower()

class AgentClassifier(AbsClassifer):
    def clean_agent(self, agent):
      agent = agent.replace(';',' ').replace('(',' ').replace(')',' ').replace(',',' ').replace('-',' ').replace('_',' ')
      agent_segs = agent.split(' ')
      agent_segs = map(lambda agent_seg: re.sub('[/]?[0-9][0-9.]*', '', agent_seg), agent_segs)
      agent_segs = filter(None, map(lambda agent_seg: re.sub('  *', ' ', agent_seg.strip()), agent_segs))
      return agent_segs

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
      # agent_segs = self.clean_agent(pkg.agent)
      # map(lambda agent_seg: self.agentLabel[agent_seg].add(label), agent_segs)
      agent = re.sub('[/].*', '', pkg.agent)
      self.agentLabel[agent].add(label)
      self.agentLabel[label].add(label)

    # def count(self, pkg):
    #   def addCommonStr(agent, label, feature_str):
    #     common_str = longest_common_substring(agent.lower(), feature_str.lower())
    #     if len(common_str) > 2:
    #         self.agentLabel[common_str].add(label)

    #   label = pkg.label
    #   agent = pkg.agent
    #   package = pkg.appInfo.package
    #   company = pkg.appInfo.company
    #   name = pkg.appInfo.name
    #   map(lambda feature_str: addCommonStr(agent, label, feature_str), [package, company, name])
    #   agent = self.clean_agent(agent)
    #   map(lambda feature_str: self.agentLabel[feature_str].add(label), filter(None, agent.split(' ')))
    #   agent = re.sub('[/].*', '', pkg.agent)
    #   self.agentLabel[agent].add(label)

    def _clean_db(self, ruleType):
      QUERY = consts.SQL_DELETE_AGENT_RULES
      print QUERY
      sqldao = SqlDao()
      sqldao.execute(QUERY % (ruleType))
      sqldao.close()

    def train(self, records, ruleType):
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

          self.rules[ruleType][agent] = label

          if agent == test_str:
            print 'Rule Type is', ruleType 


      print 'number of rule', len(self.rules[consts.APP_RULE])
      print 'persist'
      self.persist(self.rules, ruleType)
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
        agent = re.sub('[/].*', '', pkg.agent)
        label = self.rules[ruleType].get(agent, None)
        if not label:
          wordList = backward_maxmatch(pkg.agent, set(self.rules[ruleType].keys()), len(pkg.agent), 2)
          longestWord = max(wordList, key = lambda x: len(x)) if len(wordList) > 1 else ''
          label = self.rules[ruleType].get(longestWord, None)
          print wordList, longestWord, label

        rst[ruleType] = (label, 1.0)
        if label != None and label != pkg.app:
            print pkg.agent, pkg.app, label
      return rst
