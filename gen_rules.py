from classifier_factory import classifier_factory
import argparse
import consts
import re
import sys
ruleTmplate = 'F-SBID( --vuln_id %s; --attack_id %s; --name "%s"; --revision %s; --group %s; --protocol %s; --service %s; --flow %s; --weight %s; %s)\n'
patternTmplate = '--%s "/%s/i"; --context %s; '
PATTERN = 'pattern'
PCRE = 'pcre'
IOS_GROUP = 'ios_app'
class Rule:
  def __init__(self, vulnID, name, group, weight):
    self.vulnID = vulnID
    self.name = name
    self.group = group
    self.attachID = 1
    self.revision = 1
    self.group = group
    self.protocal = 'tcp'
    self.service = 'HTTP'
    self.flow = 'from_client'
    self.weight = weight
    self.features = []
  
  def add_feature_str(self, patternType, featureStr, context):
    self.features.append((patternType, featureStr, context))

  def to_string(self):
    patternStr = ''
    for patternType, featureStr, context in self.features:
      patternStr += patternTmplate % (patternType, featureStr, context)
    return ruleTmplate % (self.vulnID, self.attachID, self.name, self.revision, self.group, self.protocol, self.service, self.flow, self.weight, patternStr)

def output_rules(name, rules):
  fileWriter = open(name, 'w')
  fileWriter.write('# IDS rule version=6.639 2015/05/04 11:23:50  syntax=1  fortios=501\n')
  fileWriter.write('F-SGROUP( --name ios_app; )\n')
  for rule in rules:
    fileWriter.write(rule.to_string()+'\n')
  fileWriter.close()

def generate_agent_rules():
  trainedClassifiers = [
      #consts.HEAD_CLASSIFIER,
      consts.AGENT_CLASSIFIER,
      #consts.HOST_CLASSIFIER,
      #consts.CMAR_CLASSIFIER,
      #consts.KV_CLASSIFIER,
  ]

  appType = consts.IOS
  classifier = classifier_factory(trainedClassifiers, appType)[0][1]
  classifier.load_rules()
  vulnID = 100001
  iosGroup = 'ios_app'
  rules = []
  for ruleType in classifier.rules:
    for agentFeature, label in classifier.rules[ruleType].items():
      if len(label) > 1:
        rule = Rule(vulnID, label, IOS_GROUP, len(agentFeature))
        patternRegex = re.escape('User-Agent:')+'.*' + re.escape(agentFeature)
        rule.add_feature_str(PCRE, patternRegex, 'head')
        rules.append(rule)
        vulnID += 1
  output_rules('agent.rule.head', rules)

def generate_host_rules():
  trainedClassifiers = [
      #consts.HEAD_CLASSIFIER,
      #consts.AGENT_CLASSIFIER,
      consts.HOST_CLASSIFIER,
      #consts.CMAR_CLASSIFIER,
      #consts.KV_CLASSIFIER,
  ]

  appType = consts.IOS
  classifier = classifier_factory(trainedClassifiers, appType)[0][1]
  classifier.load_rules()
  vulnID = 200001
  iosGroup = 'ios_app'
  rules = []
  for ruleType in classifier.rules:
    for host, label in classifier.rules[ruleType].items():
      pattern = host
      rule = Rule(vulnID, label, IOS_GROUP, 9)
      rule.add_feature_str(PCRE, pattern, 'host')
      vulnID += 1
      rules.append(rule)
  output_rules('host.rule.head', rules)

def generate_path_rules():
  trainedClassifiers = [
      #consts.HEAD_CLASSIFIER,
      #consts.AGENT_CLASSIFIER,
      #consts.HOST_CLASSIFIER,
      consts.CMAR_CLASSIFIER,
      #consts.KV_CLASSIFIER,
  ]

  appType = consts.IOS
  classifier = classifier_factory(trainedClassifiers, appType)[0][1]
  classifier.load_rules()
  vulnID = 300001
  iosGroup = 'ios_app'
  
  rules = []
  for ruleType in classifier.rules:
    for cmarFeature, label in classifier.rules[ruleType].items():
      rule = Rule(vulnID, label, IOS_GROUP, 7)
      for feature in cmarFeature:
        feature = re.escape(feature)
        rule.add_feature_str(PCRE, feature, 'uri')
      vulnID += 1
      rules.append(rule)
  output_rules('cmar.rule.head', rules)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Generate rule in ips format')
    parser.add_argument('-t', metavar='agent/host/path', help='rule type')
    # parser.add_argument('-apptype', metavar='apptype', help='apptype')
    args = parser.parse_args()

    test_tbl = None
    if args.t == 'agent':
      generate_agent_rules()
    elif args.t == 'host':
      generate_host_rules()
    elif args.t == 'path':
      generate_path_rules()
    else:
      parser.print_help()

