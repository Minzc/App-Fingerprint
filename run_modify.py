import datetime
from sqldao import SqlDao
from utils import load_pkgs, load_exp_app
from collections import namedtuple, defaultdict
import const.consts as consts
import sys
import argparse
from rules.rule_manager import RuleManager
from classifiers.classifier_factory import classifier_factory


LIMIT = None
INSERT = True
PRUNE = False

VALID_LABEL = {consts.APP_RULE, consts.COMPANY_RULE, consts.CATEGORY_RULE}
TRAIN_LABEL = {
  consts.APP_RULE, 
  #consts.COMPANY_RULE, 
  #consts.CATEGORY_RULE
}

USED_CLASSIFIERS = [
    #consts.HEAD_CLASSIFIER,
    consts.AGENT_CLASSIFIER,
    consts.HOST_CLASSIFIER,
    consts.CMAR_CLASSIFIER,
    consts.KV_CLASSIFIER,
]


def merge_rst(rst, tmprst):
    for pkg_id, predictions in tmprst.iteritems():
      if pkg_id not in rst:
        rst[pkg_id] = predictions
      else:
        for rule_type in VALID_LABEL:
          if rst[pkg_id][rule_type].label == None:
            rst[pkg_id][rule_type] = tmprst[pkg_id][rule_type]
    return rst


def load_data_set(trainTbls, appType):
  """
  Load data from given table
  Input
  - trainTbls : a list of tables
  - appType : IOS or ANDROID
  Output
  - record : {table_name : [list of packages]}
  """
  print 'Loading data set', trainTbls
  expApp = load_exp_app()
  def _keep_exp_app(package):
    return package.app in expApp[appType]
  records = {}
  for tbl in trainTbls:
    records[tbl] = load_pkgs(limit = LIMIT, filterFunc = _keep_exp_app, DB = tbl, appType = appType)
    
  return records

def train(trainTbls, appType):
  '''
  1. Load data from database from given tables
  2. Train classifiers, prune and persist rules in database
  Input
  - trainTbls: A list of tables used to train classifiers
  - appType: android or ios
  '''
  trainSet = load_data_set(trainTbls, appType)
  ruleDict = {}
  for ruleType in TRAIN_LABEL:
    for tbl in trainSet:
      for pkg in trainSet[tbl]:
        if ruleType == consts.APP_RULE:
          pkg.set_label(pkg.app)
        elif ruleType == consts.COMPANY_RULE:
          pkg.set_label(pkg.company)
        elif ruleType == consts.CATEGORY_RULE:
          pkg.set_label(pkg.category)

    classifiers = classifier_factory(USED_CLASSIFIERS, appType)
    for name, classifier in classifiers:
        print ">>> [train#%s] " % (name)
        classifier.train(trainSet, ruleType)
  trainSet = None # To release memory

  print '>>> Finish training all classifiers'
  if PRUNE:
    print '>>> Start rule pruning'
    ruleManager = RuleManager()
    if consts.CMAR_CLASSIFIER in USED_CLASSIFIERS:
      classifiers["CMAR Rule"].rules = ruleManager.pruneCMARRules(ruleDict['CMAR Rule'], ruleDict['Host Rule'])
      classifiers["CMAR Rule"].persist()
    if 'KV Rule' in classifiers:
      classifierDict["KV Rule"].rules = ruleManager.pruneKVRules(ruleDict['KV Rule'],ruleDict['Host Rule'] )
      classifierDict["KV Rule"].persist()


def evaluate(rst, testSet, testApps):
  """
  Compare predictions with test data set
  Input:
  - rst : Predictions got from test. {pkgId : {ruleType : prediction}}
  - testSet : Test data set. {pkgId : pacakge}
  - testApps : Tested apps
  Output:
  - InforTrack : contains evaluation information
  """
  # app_rst, record_id
  inforTrack = { consts.DISCOVERED_APP : 0.0, consts.PRECISION : 0.0, consts.RECALL : 0.0}
  correct, recall = 0, 0
  correctApp = set()
  for pkgId, predictions in rst.items():
    predictApp = predictions[consts.APP_RULE].label 
    predictCompany = predictions[consts.COMPANY_RULE].label
    predictCategory = predictions[consts.CATEGORY_RULE].label
    ifCorrect = True
    if predictApp != None and predictApp != testSet[pkgId].app:
      ifCorrect = False
    if predictCompany != None and predictCompany != testSet[pkgId].company:
      ifCorrect = False
    if predictCategory != None and predictCategory != testSet[pkgId].category:
      ifCorrect = False

    if sum([1 for value in predictions.values() if value != consts.NULLPrediction]) > 0:
      recall += 1
      if ifCorrect:
        correct += 1
        correctApp.add((testSet[pkgId].app, testSet[pkgId].appInfo.trackId))
    
  print '[TEST] Total:', len(testSet)
  print '[TEST] Recall:', recall
  print '[TEST] Correct:', correct
  print '[TEST] Discoered Number of App:', len(correctApp)
  print '[TEST] Total Number of App:', len(testApps)

  precision = correct * 1.0 / recall
  recall = recall * 1.0 / len(testSet)
  f1Score = 2.0 * precision * recall / (precision + recall)
  inforTrack[consts.DISCOVERED_APP] = len(correctApp) * 1.0 / len(testApps)
  inforTrack[consts.PRECISION] = precision
  inforTrack[consts.RECALL] = recall
  inforTrack[consts.F1SCORE] = f1Score
  inforTrack[consts.DISCOVERED_APP_LIST] = correctApp
  return inforTrack



def _use_classifier(classifier, testSet):
  """
  Use trained classifer on the given test data set
  Input
  - classifier : classifier used on the data set
  - testSet : testData set {pkgId : package}
  """
  rst = defaultdict(dict)
  for pkg_id, record in testSet.items():
      # predict
      labelDists = classifier.classify(record)
      for labelType, prediction in labelDists.iteritems():
        # TODO need to do result selection
        if labelType in VALID_LABEL:
            rst[pkg_id][labelType] = prediction
  return rst


def _insert_rst(rst, DB):
  """
  Insert prediction results into data base
  Input
  - rst : prediction results. {pkgId: {ruleType: prediction}}
  - DB : inserted table name
  """
  print 'Start inserting results'
  QUERY = 'UPDATE ' + DB + ' SET classified = %s WHERE id = %s'
  sqldao = SqlDao()
  params = []
  for k, v in rst.iteritems():
    for ruleType in VALID_LABEL:
      if v[ruleType][0]:
        params.append((3,k));
        break
  sqldao.executeBatch(QUERY,  params)
  sqldao.close()
  print 'Finish inserting %s items' % len(rst)


def _clean_up():
  sqldao = SqlDao()
  sqldao.execute(consts.SQL_CLEAN_ALL_RULES)
  sqldao.close()
  print consts.SQL_CLEAN_ALL_RULES

def test(testTbl, appType):
  testSet = {}
  testApps = set()
  for record in load_data_set([testTbl], appType).values()[0]:
    testSet[record.id] = record
    testApps.add(record.app)

  testSize = len(testSet)
  rst = {}
  classifiers = classifier_factory(USED_CLASSIFIERS, appType)
  for name, classifier in classifiers:
    print ">>> [test#%s] " % (name)
    classifier.load_rules()
    tmprst = _use_classifier(classifier, testSet)
    rst = merge_rst(rst, tmprst)
    recall = sum([1 for i in rst.values() if i[consts.APP_RULE][0] or i[consts.COMPANY_RULE][0] or i[consts.CATEGORY_RULE][0]])
    print ">>> Recognized: %s Test Size: %s" % (recall, testSize)

  print '>>> Start evaluating'
  inforTrack = evaluate(rst, testSet, testApps)
  return inforTrack



def cross_batch_test(trainTbls, testTbl, appType, ifTrain = True):

  print '>>> Start training'
  if ifTrain:
    _clean_up()
    train(trainTbls, appType)
  print '>>> Start testing'
  inforTrack = test(testTbl, appType)

  precision = inforTrack[consts.PRECISION]
  recall = inforTrack[consts.RECALL]
  appCoverage = inforTrack[consts.DISCOVERED_APP]
  f1Score = inforTrack[consts.F1SCORE]
  print 'Precision %s, Recall: %s, App: %s, F1 Score: %s' % (precision, recall, appCoverage, f1Score)
  return inforTrack


######### START ###########

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('-t', metavar='cross/single', help='test type')
    parser.add_argument('-train', metavar='tablename', nargs='+', help='train set')
    parser.add_argument('-test', metavar='tablename', help='test set')
    parser.add_argument('-apptype', metavar='apptype', help='test apptype')
    args = parser.parse_args()

    testTbl = None
    if args.t == 'cross':
      if args.apptype.lower() == 'ios':
        appType = consts.IOS
      elif args.apptype.lower() == 'android':
        appType = consts.ANDROID
      cross_batch_test(args.train, args.test, appType)
