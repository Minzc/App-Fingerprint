from utils import longest_common_substring, backward_maxmatch
from sqldao import SqlDao
from collections import defaultdict
import const.consts as consts
from classifier import AbsClassifer
import re
from const.app_info import AppInfos

test_str = 'NBC'.lower()
VALID_FEATURES = {'CFBundleName', 'CFBundleExecutable', 'CFBundleIdentifier', 'CFBundleDisplayName', 'CFBundleURLSchemes'}
class AgentClassifier(AbsClassifer):
  def _load_info_features(self):
    from os import listdir
    from os.path import isfile, join
    folder = './resource/Infoplist/'
    self.appFeatures = defaultdict(set)
    for f in listdir(folder):
      filePath = join(folder, f)
      if isfile(filePath):
        trackId = f[0:-4]
        app = AppInfos.get(consts.IOS, trackId).package
        features = self._parse_xml(filePath )
        features.add(app)
        self.appFeatures[app] = features
  def _unescape(self, s):
    s = s.replace("&lt;", "<")
    s = s.replace("&gt;", ">")
    # this has to be last:
    s = s.replace("&amp;", "&")
    return s

  def _parse_xml(self, filePath):
    import plistlib
    plistObj = plistlib.readPlist(filePath)
    features = set()
    for key in VALID_FEATURES:
      if key in plistObj:
        value = self._unescape(plistObj[key].lower())
        features.add(value)
    return features

  def _parse_xml2(self, filePath):
    import plistlib
    plistObj = plistlib.readPlist(filePath)
    def _flat(plistObj):
      values = set()
      if type(plistObj) == plistlib._InternalDict:
        for key, value in plistObj.items():
          if type(value) == list:
            values |= _flat(value)
          elif type(value) == plistlib._InternalDict:
            values |= _flat(value)
          elif type(value) == str:
            values.add(value)
          elif type(value) == unicode:
            print 'type:', type(value)
            print 'value:', value.encode('utf-8')
          else:
            print 'type:', type(value)
            print 'value:', value
      elif type(plistObj) == list:
        for value in plistObj:
          if type(value) == list:
            values |= _flat(value)
          elif type(value) == plistlib._InternalDict:
            values |= _flat(value)
          elif type(value) == str:
            values.add(value)
          elif type(value) == unicode:
            print 'type:', type(value)
            print 'value:', value.encode('utf-8')
          else:
            print 'type:', type(value)
            print 'value:', value
      return values
    values = _flat(plistObj)
    for value in values:
      try:
        print filePath, value.encode('utf-8')
      except:
        pass
    return values



  def clean_agent(self, agent):
    agent = re.sub(r'\b[0-9][0-9._]*\b', '', agent)
    agent = re.sub(r'  *', ' ', agent)
    return agent

  def split_agent(self, agent):
    return re.findall('[a-zA-Z0-9][0-9a-zA-Z. %_\-:&?\']+/\[version\]', agent)

  def split_agent_no_slash(self, agent):
    return re.findall('[a-zA-Z0-9][0-9a-zA-Z. %_\-:&?\']+ \[version\]', agent)

  def __init__(self):
    self.agentLabel = defaultdict(set)
    self.rules = defaultdict(dict)
    self._load_info_features()
  
  def persist(self, patterns, ruleType):
    '''
    Input
    - regexApp : {regex: {app1, app2}}
    '''
    self._clean_db(ruleType)
    sqldao = SqlDao()
    QUERY = consts.SQL_INSERT_AGENT_RULES
    params = []
    for regex, apps in patterns.iteritems():
      if len(apps) == 1:
        for app in apps:
          params.append((app, 1, 1, regex, consts.APP_RULE))
    sqldao.executeBatch(QUERY, params)
    sqldao.close()

  def count(self, pkg):
    label = pkg.label
    # agent = self.clean_agent(pkg.agent)
    agent = pkg.agent
    agentF = re.sub('[/].*', '', agent)
    if '/' in pkg.agent:
      agentF = '^' + agentF + '/'

    self.agentLabel[label].add(label)
    if label not in agentF:
      self.agentLabel[agentF.strip()].add(label)

    if 'freewheeladmanager' in agentF:
      print pkg.agent

    agent_segs = self.split_agent(agent)
    map(lambda seg: self.agentLabel[seg.strip()].add(label), filter(lambda seg : len(seg) > 3 and label not in seg, agent_segs))
  

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
        if agentFeatureA+'/' in self.rules[ruleType]:
          ifAdd = False
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


  def foldTest(self):
    tbls = [ 'ios_packages_2015_06_08', 'ios_packages_2015_08_10', 'ios_packages_2015_08_12', 'ios_packages_2015_08_04', 'ios_packages_2015_09_14']
    for testTbl in tbls:
      trainTbls = []
      for tbl in tbls:
        if tbl != testTbl:
          trainTbls.append(tbl)
      self.train(trainTbls, testTbl)
    
  def _gen_regex(self, feature, app):
    regex = set()
    regex.add(r'\b' + re.escape(feature+'/'))
    regex.add(r'^' + re.escape(feature+'/'))
    regex.add(r'\b' + re.escape(feature) + r' \b[vr]?[0-9.]+\b')
    regex.add(r'\b' + re.escape(app)+ r'\b')
    regex.add(r'\b' + re.escape(feature)+ r'\b')
    return regex
  
  def _compose_regxobj(self, agentTuples):
    '''
    Compose regular expression
    '''
    appFeatureRegex = defaultdict(lambda : {})
    for app, agent in agentTuples:
      for f in self.appFeatures[app]:
        for feature in self._gen_features(f):
          if feature in agent.encode('utf-8'):
            for regex in self._gen_regex(feature, app):
              regexObj = re.compile(regex, re.IGNORECASE)
              appFeatureRegex[app][regexObj.pattern] = regexObj

      if '/' in agent:
        feature = re.findall('^[a-zA-Z0-9][0-9a-zA-Z. %_\-:&?\']+/', agent)[0]
        regexObj = re.compile(r'^' + re.escape(feature), re.IGNORECASE)
        appFeatureRegex[app]['#'+ feature] = regexObj
    return appFeatureRegex
    
  def _count(self, appFeatureRegex, appAgent):
    '''
    Count regex
    '''
    regexApp = defaultdict(set)
    for app, agents in appAgent.items():
      for agent in agents:
        for predict, patternNregexObjs in appFeatureRegex.items():
          for pattern, regexObj in patternNregexObjs.items():
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
    print 'NEW AGENT METHOD'
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

    self.persist(regexApp, 1)


  def load_rules(self):
    import re
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
        match = regex.search(pkg.agent)
        if match:
          if len(longestWord) < len(agentF):
            rstLabel = label
            longestWord = agentF

      rst[ruleType] = consts.Prediction(rstLabel, 1.0, longestWord) if rstLabel else consts.NULLPrediction
      
      if rstLabel != None and rstLabel != pkg.app and ruleType == consts.APP_RULE:
        print '>>>[AGENT CLASSIFIER ERROR] agent:', pkg.agent, 'App:',pkg.app, 'Prediction:',rstLabel, 'Longestword:',longestWord
    return rst

if __name__ == '__main__':
  agent = AgentClassifier()
  agent.foldTest()

