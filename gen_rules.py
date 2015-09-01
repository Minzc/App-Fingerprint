from classifier_factory import classifier_factory
import consts
import re
import sys
ruleTmplate = 'F-SBID( --vuln_id %s; --attack_id %s; --name "%s"; --revision %s; --group %s; --protocol %s; --service %s; --flow %s; --pcre "/%s/i"; --context %s;  --weight %s;)\n'
def generate_agent_rules():
  fileWriter = open('agent.rule.head', 'w')
  fileWriter.write('# IDS rule version=6.639 2015/05/04 11:23:50  syntax=1  fortios=501\n')
  fileWriter.write('F-SGROUP( --name ios_app; )\n')
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

  for ruleType in classifier.rules:
    for agentFeature, label in classifier.rules[ruleType].items():
      if len(label) > 1:
        pcre = re.escape('User-Agent:')+'.*' + re.escape(agentFeature)
        fileWriter.write(ruleTmplate % (vulnID, 1, label, 1, iosGroup, 'tcp', 'HTTP', 'from_client', pcre, 'header', len(agentFeature)))
        vulnID += 1
  fileWriter.close()

def generate_host_rules():
  fileWriter = open('host.rule.head', 'w')
  fileWriter.write('F-SGROUP( --name ios_app; )\n')
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

  for ruleType in classifier.rules:
    for host, label in classifier.rules[ruleType].items():
      pcre = host
      pcre = re.escape(pcre)
      fileWriter.write(ruleTmplate % (vulnID, 1, label, 1, iosGroup, 'tcp', 'HTTP', 'from_client', pcre, 'host', 9))
      vulnID += 1
  fileWriter.close()

def generate_path_rules():
  fileWriter = open('host.rule.head', 'w')
  fileWriter.write('F-SGROUP( --name ios_app; )\n')
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

  for ruleType in classifier.rules:
    for agentFeature, label in classifier.rules[ruleType].items():
      pcre = 'User-Agent:.*' + agentFeature
      pcre = re.escape(pcre)
      fileWriter.write(ruleTmplate % (vulnID, 1, label, 1, iosGroup, 'tcp', 'HTTP', 'from_client', pcre, 'uri', 8))
      vulnID += 1
  fileWriter.close()

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

