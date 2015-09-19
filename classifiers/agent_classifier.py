from utils import longest_common_substring, backward_maxmatch
from sqldao import SqlDao
from collections import defaultdict
import const.consts as consts
from classifier import AbsClassifer
import re

test_str = 'NBC'.lower()

class AgentClassifier(AbsClassifer):
    def clean_agent(self, agent):
      return re.findall('[a-zA-Z][0-9a-zA-Z. %_-]+', agent)

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
      if '/' in pkg.agent:
        agent = agent + '/'
      self.agentLabel[agent].add(label)
      self.agentLabel[label].add(label)
      agent_segs = self.clean_agent(pkg.agent)
      map(lambda seg: self.agentLabel[seg].add(label), filter(lambda seg : len(seg) > 3, agent_segs))


    def _clean_db(self, ruleType):
      QUERY = consts.SQL_DELETE_AGENT_RULES
      print ">>> [Agent Classifier]", QUERY
      sqldao = SqlDao()
      sqldao.execute(QUERY % (ruleType))
      sqldao.close()
    
    def _prune(self):
      for ruleType in self.rules:
        prunedRules = {}
        for agentFeatureA in self.rules[ruleType]:
          ifAdd = True
          for agentFeatureB in self.rules[ruleType]:
            if agentFeatureA != agentFeatureB and agentFeatureB in agentFeatureA:
              if agentFeatureA == '3d world magazine: for 3d artists and animators 3.8.3 rv:3.8.3.0 (ipad; iphone os 8.4; en_ca)':
                    print '##F', agentFeatureB
                    print '##A', self.rules[ruleType][agentFeatureA] 
                    print '##B', self.rules[ruleType][agentFeatureB] 
              if self.rules[ruleType][agentFeatureA] == self.rules[ruleType][agentFeatureB]:
                ifAdd = False
                break
          if ifAdd:
            prunedRules[agentFeatureA] = self.rules[ruleType][agentFeatureA]
        self.rules[ruleType] = prunedRules

    def train(self, records, ruleType):
      for pkg in [pkg for pkgs in records.values() for pkg in pkgs]:
        self.count(pkg)
      ########################
      # Generate Rules
      ########################
      
      # print test_str in self.agentLabel

      for agent, labels in self.agentLabel.iteritems():
        if agent == test_str:
          print '#', len(labels)
          print labels
        
        if len(labels) == 1:
          label = labels.pop()

          self.rules[ruleType][agent] = label

          if agent == test_str:
            print 'Rule Type is', ruleType 


      print '>>> [Agent Classifier] Number of Rule', len(self.rules[consts.APP_RULE])
      self._prune()
      print '>>> [Agent Classifier] Number of Rule After Pruning', len(self.rules[consts.APP_RULE])

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
      longestWord = None
      for ruleType in self.rules:
        agent = re.sub('[/].*', '', pkg.agent)
        if '/' in pkg.agent:
          agent = agent + '/'
        label = self.rules[ruleType].get(agent)
        if not label:
          wordList = backward_maxmatch(pkg.agent, set(self.rules[ruleType].keys()), len(pkg.agent), 3)
          wordList = filter(lambda seg: len(self.rules[ruleType][seg]) > 1, wordList)
          longestWord = max(wordList, key = lambda x: len(x)) if len(wordList) > 0 else ''
          label = self.rules[ruleType].get(longestWord)

        rst[ruleType] = consts.Prediction(label, 1.0, longestWord) if label else consts.NULLPrediction

        # if label != None and label != pkg.app and ruleType == consts.APP_RULE:
        #   print '>>>[AGENT CLASSIFIER ERROR] agent:', pkg.agent, 'App:',pkg.app, 'Prediction:',label, 'Longestword:',longestWord
      return rst
