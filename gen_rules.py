from classifier_factory import classifier_factory
import consts
import re
def generate_agent_rules():
  fileWriter = open('agent.rule.head', 'w')
  fileWriter.write('F-SGROUP( --name ios_app; )\n')
  trainedClassifiers = [
      #consts.HEAD_CLASSIFIER,
      consts.AGENT_CLASSIFIER,
      #consts.HOST_CLASSIFIER,
      #consts.CMAR_CLASSIFIER,
      #consts.KV_CLASSIFIER,
  ]

  ruleTmplate = 'F-SBID( --vuln_id %s; --attack_id %s; --name "%s"; --revision %s; --group %s; --protocol %s; --service %s; --flow %s; --pcre "/%s/i"; --context header; )\n'
  appType = consts.IOS
  classifier = classifier_factory(trainedClassifiers, appType)[0][1]
  classifier.load_rules()
  vulnID = 100001
  iosGroup = 'ios_app'

  for ruleType in classifier.rules:
    for agentFeature, label in classifier.rules[ruleType].items():
      pcre = 'User-Agent:.*' + agentFeature
      pcre = re.escape(pcre)
      fileWriter.write(ruleTmplate % (vulnID, 1, label, 1, iosGroup, 'tcp', 'HTTP', 'from_server', pcre))
      vulnID += 1
  fileWriter.close

def generate_host_rules():

