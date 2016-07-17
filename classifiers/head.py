from classifier import AbsClassifer
import const.consts as consts


class HeaderClassifier(AbsClassifer):
  def __init__(self):
    self.name = consts.HEAD_CLASSIFIER
    self.rules = None

  def train(self, train_set, rule_type):
    return self
    
  def load_rules(self):
    pass
    
  def c(self, package):
      return {consts.APP_RULE: self._classify(package)}

  def _classify(self, package):
      identifier = ['x-umeng-sdk', 'x-vungle-bundle-id', 'x-requested-with']
      for id in identifier:
          for head_seg in package.add_header.split('\n'):
              if id in head_seg and '.' in head_seg:
                  label = head_seg.replace(id + ':', '').strip()
                  return consts.Prediction(label, 1.0, id)
      return consts.NULLPrediction
