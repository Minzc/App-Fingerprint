from utils import longest_common_substring, backward_maxmatch, unescape, flatten, load_info_features
from sqldao import SqlDao
from collections import defaultdict
import const.consts as consts
from classifier import AbsClassifer
import re
from const.app_info import AppInfos

VALID_FEATURES = {'CFBundleName', 'CFBundleExecutable', 'CFBundleIdentifier', 'CFBundleDisplayName', 'CFBundleURLSchemes'}
STRONG_FEATURES = {'CFBundleName', 'CFBundleExecutable', 'CFBundleIdentifier', 'CFBundleDisplayName'}
class AgentClassifier(AbsClassifer):
  def __init__(self):
    self.agentLabel = defaultdict(set)
    self.rules = defaultdict(dict)
    self.appFeatures = load_info_features(self._parse_xml)

  def _parse_xml(self, filePath):
    import plistlib
    plistObj = plistlib.readPlist(filePath)
    features = {}
    for key in VALID_FEATURES:
      if key in plistObj:
        value = unescape(plistObj[key].lower())
        features[key] = value
    return features
  
  def persist(self, patterns, ruleType):
    '''
    Input
    - regexApp : {regex: {app1, app2}}
    '''
    self.clean_db(ruleType, consts.SQL_DELETE_AGENT_RULES)
    sqldao = SqlDao()
    QUERY = consts.SQL_INSERT_AGENT_RULES
    params = []
    for regex, apps in patterns.iteritems():
      if len(apps) == 1:
        app = list(apps)[0]
        params.append((app, 1, 1, regex, consts.APP_RULE))
    sqldao.executeBatch(QUERY, params)
    sqldao.close()

  def _prune(self):
    for ruleType in self.rules:
      prunedRules = {}
      for agentFeatureA in self.rules[ruleType]:
        ifAdd = False if agentFeatureA+'/' in self.rules[ruleType] else True
        if ifAdd or '/' in agentFeatureA:
          prunedRules[agentFeatureA] = self.rules[ruleType][agentFeatureA]
      self.rules[ruleType] = prunedRules

  def _gen_features(self, f):
    '''
    Generate different type of feature
    '''
    import urllib
    featureSet = set()
    f = f.encode('utf-8')
    featureSet.add(f)
    featureSet.add(urllib.quote(f))
    featureSet.add(f.replace(' ', '%20'))
    featureSet.add(f.replace(' ', '-'))
    featureSet.add(f.replace(' ', '_'))
    featureSet.add(f.replace(' ', ''))
    return featureSet

  def _gen_regex(self, feature, app):
    regex = set()
    regex.add(r'\b' + re.escape(feature+'/'))
    regex.add(r'^' + re.escape(feature+'/'))
    regex.add(r'\b' + re.escape(feature) + r' \b[vr]?[0-9.]+\b')
    regex.add(r'\b' + re.escape(app)+ r'\b')
    regex.add(r'\b' + re.escape(feature)+ r'\b')
    return regex
  
  def _compose_regxobj2(self, agentTuples):
    appFeatureRegex = defaultdict(lambda : {})
    for app in self.appFeatures:
      for f in self.appFeatures[app].values():
        for feature in self._gen_features(f):
          for regex in self._gen_regex(feature, app):
            regexObj = re.compile(regex, re.IGNORECASE)
            appFeatureRegex[app][regexObj.pattern] = regexObj
    return appFeatureRegex


  def _count2(self, appFeatureRegex, appAgent):
    '''
    Count regex
    '''
    
    minedAppFeatureRegex = defaultdict(lambda : {})
    for app, agents in appAgent.items():
      for agent in agents:
        if '/' in agent:
          features = re.findall('^[a-zA-Z0-9][0-9a-zA-Z. %_\-:&?\']+/', agent)
          if len(features) > 0:
            feature = features[0]
          regexObj = re.compile(r'^' + re.escape(feature), re.IGNORECASE)
          try:
            feature = feature.encode('utf-8')
            minedAppFeatureRegex[app][feature] = regexObj
          except:
            pass
    regexApp = defaultdict(set)
    for app, agents in appAgent.items():
      for agent in agents:
        for predict, pattern, regexObj in flatten(minedAppFeatureRegex):
          if pattern in agent:
            regexApp[regexObj.pattern].add(app)

    for predict, pattern, regexObj in flatten(appFeatureRegex):
      regexApp[regexObj.pattern].add(predict)

    return regexApp

  def _compose_regxobj(self, agentTuples):
    '''
    Compose regular expression
    '''
    appFeatureRegex = defaultdict(lambda : {})
    for app, agent in agentTuples:
      for f in self.appFeatures[app].values():
        for feature in self._gen_features(f):
          if feature in agent.encode('utf-8'):
            for regex in self._gen_regex(feature, app):
              regexObj = re.compile(regex, re.IGNORECASE)
              appFeatureRegex[app][regexObj.pattern] = regexObj

      if '/' in agent:
        features = re.findall('^[a-zA-Z0-9][0-9a-zA-Z. %_\-:&?\']+/', agent)
        if len(features) > 0:
          feature = features[0]
        regexObj = re.compile(r'^' + re.escape(feature), re.IGNORECASE)
        try:
          feature = feature.encode('utf-8')
          appFeatureRegex[app]['#'+ feature] = regexObj
        except:
          pass


    return appFeatureRegex
    
  def _count(self, appFeatureRegex, appAgent):
    '''
    Count regex
    '''
    regexApp = defaultdict(set)
    for app, agents in appAgent.items():
      for agent in agents:
        for predict, pattern, regexObj in flatten(appFeatureRegex):
          regexApp[regexObj.pattern].add(predict)
          if '#' in pattern:
            pattern = pattern.replace('#', '')
            if pattern in agent:
              regexApp[regexObj.pattern].add(app)
          elif regexObj.search(agent):
            regexApp[regexObj.pattern].add(app)
          elif regexObj.search(app):
            regexApp[regexObj.pattern].add(app)
    return regexApp

  def train(self, trainSet, ruleType):
    agentTuples = set()
    appAgent = defaultdict(set)
    for tbl,pkgs in trainSet.items():
      for pkg in pkgs:
        label = pkg.label
        agent = pkg.agent
        agentTuples.add((label, agent))
        appAgent[label].add(agent)
    '''
    Compose regular expression
    '''
    appFeatureRegex = self._compose_regxobj(agentTuples)
    
    '''
    Count regex
    '''
    regexApp = self._count(appFeatureRegex, appAgent)

    self.persist(regexApp, consts.APP_RULE)


  def load_rules(self):
    self.rules = {consts.APP_RULE:{}, consts.COMPANY_RULE:{}, consts.CATEGORY_RULE:{}}
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
      
      if rstLabel != None and rstLabel != pkg.app and ruleType == consts.APP_RULE:
        print '>>>[AGENT CLASSIFIER ERROR] agent:', pkg.agent, 'App:',pkg.app, 'Prediction:',rstLabel, 'Longestword:',longestWord
    return rst

