from package import Package
from sqldao import SqlDao
from utils import load_pkgs
import consts
from abc import ABCMeta, abstractmethod

DEBUG = False
class AbsClassifer:
  __metaclass__ = ABCMeta
  @abstractmethod
  def classify(self, package): pass
  
  @abstractmethod
  def train(self, train_set, rule_type): pass

  @abstractmethod
  def load_rules(self): pass



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
    rst = {consts.APP_RULE: identifier, consts.COMPANY_RULE: (None, 1), consts.CATEGORY_RULE:(None, 1)}

    if identifier[0] != package.app:
      if DEBUG : print identifier, package.app
    return rst

  def _classify(self, package):
      identifier = ['x-umeng-sdk', 'x-vungle-bundle-id', 'x-requested-with']
      for id in identifier:
          for head_seg in package.add_header.split('\n'):
              if id in head_seg and '.' in head_seg:
                  return (head_seg.replace(id + ':', '').strip(), 1)
      return (None, 1)

if __name__ == '__main__':
  classify(True)

