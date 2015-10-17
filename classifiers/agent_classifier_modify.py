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
      else:
        print filePath, key
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
    import urllib
    featureSet = set()
    f = f.encode('utf-8')
    featureSet.add(f)
    featureSet.add(urllib.quote(f))
    featureSet.add(f.replace(' ', '%20'))
    featureSet.add(f.replace(' ', '-'))
    featureSet.add(f.replace(' ', '_'))
    return featureSet


  def foldTest(self):
    tbls = [ 'ios_packages_2015_06_08', 'ios_packages_2015_08_10', 'ios_packages_2015_08_12', 'ios_packages_2015_08_04', 'ios_packages_2015_09_14']
    for testTbl in tbls:
      trainTbls = []
      for tbl in tbls:
        if tbl != testTbl:
          trainTbls.append(tbl)
      self.train(trainTbls, testTbl)
      break

  def train(self, trainTbls, testTbl):
    from sqldao import SqlDao
    print 'Start Training'
    SQL = 'select app, agent, company from %s'
    sqldao = SqlDao()
    agentTuples = set()
    appCompany = {}
    appAgent = defaultdict(set)
    for tbl in trainTbls:
      for app, agent, company in sqldao.execute(SQL % tbl):
        appAgent[app].add(agent.lower())
        agentTuples.add((app.lower(),  agent.lower()))
        appCompany[app] = company
    
    testAgentTuples = set()
    testAppAgent = defaultdict(set)
    for app, agent, company in sqldao.execute(SQL % testTbl):
      testAgentTuples.add((app.lower(),  agent.lower()))
      testAppAgent[app].add(agent.lower())

    appFeatureRegex = defaultdict(lambda : {})
    for app, agent in agentTuples:
      for f in self.appFeatures[app]:
        for feature in self._gen_features(f):
          feature = feature.lower()
          if feature in agent.encode('utf-8'):
            regexObj = re.compile(r'\b' + re.escape(feature+'/'), re.IGNORECASE)
            appFeatureRegex[app][regexObj.pattern] = regexObj

            regexObj = re.compile(r'^' + re.escape(feature+'/'), re.IGNORECASE)
            appFeatureRegex[app][regexObj.pattern] = regexObj

            regexObj = re.compile(r'\b' + re.escape(feature) + r' \b[vr]?[0-9.]+\b', re.IGNORECASE)
            appFeatureRegex[app][regexObj.pattern] = regexObj
            
            regexObj = re.compile(r'\b' + re.escape(app)+ r'\b', re.IGNORECASE)
            appFeatureRegex[app][regexObj.pattern] = regexObj

            regexObj = re.compile(r'\b' + re.escape(feature)+ r'\b', re.IGNORECASE)
            appFeatureRegex[app][regexObj.pattern] = regexObj

      if '/' in agent:
        feature = re.sub('[/].*', '', agent)
        regexObj = re.compile(r'^' + re.escape(feature+'/'), re.IGNORECASE)
        appFeatureRegex[app]['#'+ feature] = regexObj

    regexApp = defaultdict(set)
    for app, agents in appAgent.items():
      for agent in agents:
        for predict, patternNregexObjs in appFeatureRegex.items():
          for pattern, regexObj in patternNregexObjs.items():
            if '#' in pattern:
              if pattern in agent:
                regexApp[regexObj.pattern].add(app)
            elif regexObj.search(agent):
              regexApp[regexObj.pattern].add(app)

    
    corrects = set()
    wrongs = set()
    notCovered = set()
    correctApp = set()
    wrongApp = set()
    for app, agents in testAppAgent.items():
      for agent in agents:
        for regexStr, predictApps in regexApp.items():
          regexObj = re.compile(regexStr)
          if '6f68888n5z.us.pandav.iwmata' in app and '\.iwmata' in regexStr:
            print '##', predictApps
            print '$$', regexObj.search(agent)
          if len(predictApps) == 1 and regexObj.search(agent):
            for predict in predictApps:
              if app == predict:
                print '[CORRECT]', regexObj.pattern, agent
                corrects.add(agent)
                correctApp.add(app)
              else:
                print '[WRONG]', regexObj.pattern, agent, '[APP]', app, '[PREDICT]', predict
                wrongs.add(agent)
                wrongApp.add(app)
        if agent not in corrects and agent not in wrongs:
          notCovered.add(agent)

    print '========Correct========='
    for agent in corrects:
      print '[CORRECT]', agent
    print '========Wrong========='
    for agent in wrongs:
      print '[WRONG]', agent
    print '========NOTCOVER========='
    for agent in notCovered:
      print '[NOTCOVERED]', agent
    print '========REGEX============'
    for agent, patternNregexObjs in appFeatureRegex.items():
      for pattern, regexObj in patternNregexObjs.items():
        print agent, 'REGEX', regexObj.pattern
    print '========STAT============='
    print 'Train:', trainTbls, 'Test:', testTbl
    print 'TOTAL:', len(testAppAgent),'Correct:', len(correctApp - wrongApp), 'Discover:', len(correctApp)

    



  def train2(self, records, ruleType):
    for pkg in [pkg for pkgs in records.values() for pkg in pkgs]:
      self.count(pkg)
    for pkgs in records.values():
      for pkg in pkgs:
        for seg in self.agentLabel:
          if seg in pkg.agent:
            self.agentLabel[seg].add(pkg.app)

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

  def load_rules2(self):
    self.rules = {consts.APP_RULE:{}, consts.COMPANY_RULE:{}, consts.CATEGORY_RULE:{}}
    QUERY = consts.SQL_SELECT_AGENT_RULES
    sqldao = SqlDao()
    counter = 0
    for agent, label, ruleType in sqldao.execute(QUERY):
      counter += 1
      self.rules[ruleType][agent] = label
    print '>>> [Agent Rules#loadRules] total number of rules is', counter, 'Type of Rules', len(self.rules)
    sqldao.close()

  def load_rules(self):
    import re
    self.rules = {consts.APP_RULE:{}, consts.COMPANY_RULE:{}, consts.CATEGORY_RULE:{}}
    QUERY = consts.SQL_SELECT_AGENT_RULES
    sqldao = SqlDao()
    counter = 0
    for agentF, label, ruleType in sqldao.execute(QUERY):
      counter += 1

      if '^' in agentF:
        self.rules[ruleType][agentF] = ( re.compile(r'\b' + re.escape(agentF[1:]) + r'\b'),label)
      else:
        self.rules[ruleType][agentF] = ( re.compile(r'\b' + re.escape(agentF) + r'\b'),label)
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

  def classify2(self, pkg):
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

      if label != None and label != pkg.app and ruleType == consts.APP_RULE:
        print '>>>[AGENT CLASSIFIER ERROR] agent:', pkg.agent, 'App:',pkg.app, 'Prediction:',label, 'Longestword:',longestWord
    return rst

if __name__ == '__main__':
  agent = AgentClassifier()
  agent.foldTest()

