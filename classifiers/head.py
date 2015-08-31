from classifier import AbsClassifer
import consts


class HeaderClassifier(AbsClassifer):
  def __init__(self):
    self.name = consts.HEAD_CLASSIFIER
    self.rules = None

  def train(self, train_set, rule_type):
    return self
    
  def load_rules(self):
    pass
    
  def classify(self,package):
    rst = {}
    app, company, id = package.app, package.company, package.id

    identifier = self._classify(package)
    rst = {consts.APP_RULE: identifier, consts.COMPANY_RULE: consts.NULLPrediction, consts.CATEGORY_RULE:NULLPrediction}

    return rst

  def _classify(self, package):
      identifier = ['x-umeng-sdk', 'x-vungle-bundle-id', 'x-requested-with']
      for id in identifier:
          for head_seg in package.add_header.split('\n'):
              if id in head_seg and '.' in head_seg:
                  label = head_seg.replace(id + ':', '').strip()
                  prediction = consts.Prediction(label, 1.0, id)
      return consts.NULLPrediction
