from abc import ABCMeta, abstractmethod
from sqldao import SqlDao
import const.consts as consts
from const.dataset import DataSetIter as DataSetIter

class AbsClassifer:
    __metaclass__ = ABCMeta
    def classify(self, testSet):
        batchPredicts = {}
        for tbl, pkg in DataSetIter.iter_pkg(testSet):
            predictRst = self.c(pkg)
            batchPredicts[pkg.id] = predictRst

        return batchPredicts
  
    @abstractmethod
    def train(self, train_set, rule_type):
        return

    @abstractmethod
    def load_rules(self):
        return

    @abstractmethod
    def c(self, pkg):
        return

    def set_name(self, name):
      self.name = name

    def clean_db(self, ruleType, QUERY):
        print ">>> [%s Classifier]" % (self.name), QUERY
        sqldao = SqlDao()
        sqldao.execute(QUERY % (ruleType))
        sqldao.close()


