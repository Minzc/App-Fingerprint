from abc import ABCMeta, abstractmethod

class AbsClassifer:
  __metaclass__ = ABCMeta
  @abstractmethod
  def classify(self, package): pass
  
  @abstractmethod
  def train(self, train_set, rule_type): pass

  @abstractmethod
  def load_rules(self): pass

