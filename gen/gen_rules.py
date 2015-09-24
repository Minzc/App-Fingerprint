import argparse
import const.consts as consts
import re
import sys
from const.app_info import AppInfos
from classifiers.classifier_factory import classifier_factory

ruleTmplate = 'F-SBID( --vuln_id %s; --attack_id %s; --name "%s"; --revision %s; --group %s; --protocol %s; --service %s; --flow %s; --weight %s; %s)\n'
patternTmplate = '--%s "/%s/i"; --context %s; '
PATTERN = 'pattern'
PCRE = 'pcre'
IOS_GROUP = 'ios_app'
HTTP_GET = '­­parsed_type HTTP_GET;'
class Rule:
  def __init__(self, vulnID, name, group, weight):
    trackID = AppInfos.get(consts.IOS, name).trackId
    self.vulnID = vulnID
    self.group = group
    self.attachID = 1
    self.revision = 1
    self.group = group
    self.protocol = 'tcp'
    self.service = 'HTTP'
    self.flow = 'from_client'
    self.weight = min(weight, 255)
    self.name = '[%s][%s]%s' % (self.weight, trackID, name)
    self.features = []
  
  def add_feature_str(self, patternType, featureStr, context, parsedType = None):
    self.features.append((patternType, featureStr, context, parsedType))

  def to_string(self):
    wholePatternStr = ''
    for patternType, featureStr, context, parsedType in self.features:
      patternStr = patternTmplate % (patternType, featureStr, context)
      if parsedType:
        patternStr = parsedType + patternStr
      wholePatternStr += patternStr


    return ruleTmplate % (self.vulnID, self.attachID, self.name, self.revision, self.group, self.protocol, self.service, self.flow, self.weight, patternStr)

def output_rules(name, rules):
  fileWriter = open(name, 'w')
  fileWriter.write('# IDS rule version=6.639 2015/05/04 11:23:50  syntax=1  fortios=501\n')
  fileWriter.write('F-SGROUP( --name %s; )\n' % IOS_GROUP)
  for rule in rules:
    fileWriter.write(rule.to_string().encode('utf-8')+'\n')
  fileWriter.close()

def generate_agent_rules(vulnID = 100000):
  trainedClassifiers = [ consts.AGENT_CLASSIFIER ]

  appType = consts.IOS
  classifier = classifier_factory(trainedClassifiers, appType)[0][1]
  classifier.load_rules()
  iosGroup = IOS_GROUP
  rules = []
  for ruleType in classifier.rules:
    for agentFeature, regxNlabel in classifier.rules[ruleType].items():
      if len(agentFeature) > 1:
        regex, label = regxNlabel
        rule = Rule(vulnID, label, IOS_GROUP, 41 - 1/float(len(agentFeature)))
        patternRegex = re.escape('User-Agent:')+'.*' + regex.pattern
        rule.add_feature_str(PCRE, patternRegex, 'header', HTTP_GET)
        rules.append(rule)
        vulnID += 1
  return rules

def generate_host_rules(vulnID = 200000):
  trainedClassifiers = [ consts.HOST_CLASSIFIER, ] 
  appType = consts.IOS
  classifier = classifier_factory(trainedClassifiers, appType)[0][1]
  classifier.load_rules()
  iosGroup = IOS_GROUP
  rules = []
  for ruleType in classifier.rules:
    for host, labelNsupport in classifier.rules[ruleType].items():
      label, support, regexObj = labelNsupport
      rule = Rule(vulnID, label, IOS_GROUP, 30 + support)
      pattern = re.escape(host)
      rule.add_feature_str(PCRE, pattern, 'host')
      vulnID += 1
      rules.append(rule)
  return rules

def generate_kv_rules(vulnID = 300000):
  trainedClassifiers = [ consts.KV_CLASSIFIER ]
  appType = consts.IOS
  classifier = classifier_factory(trainedClassifiers, appType)[0][1]
  classifier.load_rules()
  iosGroup = IOS_GROUP
  
  ipsRules = []
  for ruleType, rules in classifier.rules.items():
    for host, keyValues in rules.items():
      for key, valueLabels in keyValues.items():
        for value, labelScores in valueLabels.items():
          for label, supNconf in labelScores.items():
            if len(value.split('\n')) == 1:
              support = supNconf[consts.SUPPORT]
              rule = Rule(vulnID, label, IOS_GROUP, 20 + support)
              rule.add_feature_str(PCRE, re.escape(host), 'host')
              rule.add_feature_str(PCRE, re.escape(key+'='+value), 'uri')
              ipsRules.append(rule)
              vulnID += 1
  return ipsRules


def generate_path_rules(vulnID = 400000):
  trainedClassifiers = [ consts.CMAR_CLASSIFIER ]

  appType = consts.IOS
  classifier = classifier_factory(trainedClassifiers, appType)[0][1]
  classifier.load_rules()
  vulnID = 300001
  iosGroup = IOS_GROUP
  
  rules = []
  for ruleType in classifier.rules:
    for host, patterns in classifier.rules[ruleType].items():
      for cmarFeatures, labelNsupport in patterns.items():
        label, support = labelNsupport
        rule = Rule(vulnID, label, IOS_GROUP, 10 + support)
        rule.add_feature_str(PCRE, host, 'host')
        for feature in cmarFeatures:
          feature = re.escape(feature)
          rule.add_feature_str(PCRE, feature, 'uri')
        vulnID += 1
        rules.append(rule)
  return rules

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Generate rule in ips format')
    parser.add_argument('-t', metavar='agent/host/path/kv', help='rule type')
    parser.add_argument('-p', metavar='apptype_region', help='output prefix')
    # parser.add_argument('-apptype', metavar='apptype', help='apptype')
    args = parser.parse_args()

    test_tbl = None
    if args.t == 'agent':
      rules = generate_agent_rules()
      output_rules(args.p+'_agent.rule', rules)
    elif args.t == 'host':
      rules = generate_host_rules()
      output_rules(args.p+'_host.rule', rules)
    elif args.t == 'path':
      rules = generate_path_rules()
      output_rules(args.p+'_cmar.rule', rules)
    elif args.t == 'kv':
      rules = generate_kv_rules()
      output_rules(args.p+'_kv.rule', rules)
    elif args.t == 'all':
      rules = generate_agent_rules(vulnID = 100000)
      rules += generate_host_rules(vulnID = 200000)
      rules += generate_path_rules(vulnID = 300000)
      rules += generate_kv_rules(vulnID = 400000)
      output_rules(args.p+'_all.rule', rules)
    else:
      parser.print_help()

