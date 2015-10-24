from abc import ABCMeta, abstractmethod

class AbsClassifer:
  __metaclass__ = ABCMeta
  @abstractmethod
  def classify(self, package): pass
  
  @abstractmethod
  def train(self, train_set, rule_type): pass

  @abstractmethod
  def load_rules(self): pass

  def set_name(self, name):
    self.name = name

  def clean_db(self, ruleType, QUERY):
    print ">>> [%s Classifier]" % (self.name), QUERY
    sqldao = SqlDao()
    sqldao.execute(QUERY % (ruleType))
    sqldao.close()

