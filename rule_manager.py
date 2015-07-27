from collections import defaultdict
class RuleManager:
  def __init__(self):
    pass

  def pruneKVRules(self, kvRules, hostRules):
    for ruleType in kvRules:
      print 'KV Rules, Rule Type %s Before pruning %s' % ( ruleType,len(kvRules[ruleType]))
      kvRules[ruleType] = self._pruneKVRules(kvRules[ruleType], hostRules[ruleType])
    return kvRules

  def _pruneKVRules(self, kvRules, hostRules):
    import tldextract
    rulesAfterPruned = defaultdict(lambda : defaultdict( lambda : defaultdict( lambda : defaultdict(lambda : {'score':0, 'support':0}))))

    for host in kvRules:
      extracted = tldextract.extract(host)
      secdomain = None
      if len(extracted.domain) > 0:
        secdomain = "{}.{}".format(extracted.domain, extracted.suffix)
      if host in hostRules or secdomain in hostRules:
        continue
      for key in kvRules[host]:
        for value in kvRules[host][key]:
          for label in kvRules[host][key][value]:
            currenct_score = rulesAfterPruned[''][key][value][label]['score']
            currenct_support = rulesAfterPruned[''][key][value][label]['support']
            new_score = kvRules[host][key][value][label]['score']
            new_support = kvRules[host][key][value][label]['support']
            if new_score > currenct_score:
              rulesAfterPruned[''][key][value][label] = kvRules[host][key][value][label]
            elif new_score == currenct_score and new_support > currenct_support:
              rulesAfterPruned[''][key][value][label] = kvRules[host][key][value][label]              

    return rulesAfterPruned

  def pruneCMARRules(self, cmarRules, hostRules):
    for ruleType in cmarRules:
      cmarRules[ruleType] = self._pruneCMARRules(cmarRules[ruleType], hostRules[ruleType])
    return cmarRules

  def _pruneCMARRules(self, cmarRules, hostRules):
    import tldextract
    counter = 0
    rulesAfterPruned = defaultdict( lambda : defaultdict())
    for host in cmarRules:
      extracted = tldextract.extract(host)
      secdomain = None
      if len(extracted.domain) > 0:
        secdomain = "{}.{}".format(extracted.domain, extracted.suffix)
      if host in hostRules or secdomain in hostRules:
        continue
      rulesAfterPruned[host] = cmarRules[host]
    return rulesAfterPruned


