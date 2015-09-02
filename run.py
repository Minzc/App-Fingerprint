import datetime
from sqldao import SqlDao
from utils import load_pkgs, load_exp_app
from collections import namedtuple, defaultdict
import consts
import sys
import argparse
from rules.rule_manager import RuleManager
from classifier_factory import classifier_factory


LIMIT = None
INSERT = True
FOLD = 1
DEBUG = False
DEBUG_CMAR = False

validLabel = {consts.APP_RULE, consts.COMPANY_RULE, consts.CATEGORY_RULE}
trainedLabel = {
  consts.APP_RULE, 
  #consts.COMPANY_RULE, 
  #consts.CATEGORY_RULE
}

trainedClassifiers = [
    #consts.HEAD_CLASSIFIER,
    #consts.AGENT_CLASSIFIER,
    consts.HOST_CLASSIFIER,
    #consts.CMAR_CLASSIFIER,
    #consts.KV_CLASSIFIER,
]

def load_trian(size):
    trainSet = {int(item.strip()) for item in open('train_id')}
    testSet = {i for i in range(size) if i not in trainSet}
    return trainSet, testSet

def merge_rst(rst, tmprst):
    for pkg_id, predictions in tmprst.iteritems():
      if pkg_id not in rst:
        rst[pkg_id] = predictions
      else:
        for rule_type in validLabel:
          if rst[pkg_id][rule_type].label == None:
            rst[pkg_id][rule_type] = tmprst[pkg_id][rule_type]
    return rst


def evaluate(rst, testSet):
    # app_rst, record_id
    correct, wrong, total = 0, 0, 0
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
        total += 1
        if ifCorrect:
          correct += 1
          correctApp.add(testSet[pkgId].app)
          if predictApp == None:
              print 'ERROR!!!!!!!!!!!!!', predictions
        else:
          wrong += 1
      
    print 'Total:', len(testSet), 'Recognized:', total, 'Correct:', correct, 'Wrong:', wrong
    return correct, correctApp



def use_classifier(classifier, testSet):
    rst = defaultdict(dict)
    total = 0
    recall = 0
    for pkg_id, record in testSet.items():
        if len(record.queries) > 0:
          total += 1
        # predict
        labelDists = classifier.classify(record)
        max_confidence = -1
        for labelType, prediction in labelDists.iteritems():
          if labelType in validLabel:
              recall += 1
              rst[pkg_id][labelType] = prediction
    
    return rst


def insert_rst(rst, DB = 'packages'):
    QUERY = 'UPDATE ' + DB + ' SET classified = %s WHERE id = %s'
    print QUERY
    sqldao = SqlDao()
    params = []
    for k, v in rst.items():
      for rule_type in validLabel:
        if v[rule_type][0]:
          params.append((3,k));
          break
    sqldao.executeBatch(QUERY,  params)
    sqldao.close()
    print 'insert', len(rst),"items"



def execute(trainSet, testSet, inforTrack, appType):
    sqldao = SqlDao()
    sqldao.execute(consts.SQL_CLEAN_ALL_RULES)
    sqldao.close()
    print consts.SQL_CLEAN_ALL_RULES

    print "Train:", trainSet.keys(), "Test:", len(testSet)
    correct = 0
    test_apps = set()
    rst = {}
    for record in testSet.values():
        test_apps.add(record.app)
    
    ruleDict = {}
    for ruleType in trainedLabel:
        classifiers = classifier_factory(trainedClassifiers, appType)
        for tbl in trainSet:
            for pkg in trainSet[tbl]:
                if ruleType == consts.APP_RULE:
                    pkg.set_label(pkg.app)
                elif ruleType == consts.COMPANY_RULE:
                    pkg.set_label(pkg.company)
                elif ruleType == consts.CATEGORY_RULE:
                    pkg.set_label(pkg.category)

        for name, classifier in classifiers:
            print ">>> [train#%s] " % (name)
            classifier =  classifier.train(trainSet, ruleType)
    trainSet = None # To release memory
    

    # ruleManager = RuleManager()
    print '>>> Finish training all classifiers'
    print '>>> Start rule pruning'
    
    if 'CMAR Rule' in classifiers:
        classifiers["CMAR Rule"].rules = ruleManager.pruneCMARRules(ruleDict['CMAR Rule'], ruleDict['Host Rule'])
        classifiers["CMAR Rule"].persist()
    if 'KV Rule' in classifiers:
        classifierDict["KV Rule"].rules = ruleManager.pruneKVRules(ruleDict['KV Rule'],ruleDict['Host Rule'] )
        classifierDict["KV Rule"].persist()
    
    for name, classifier in classifiers:
        print ">>> [test#%s] " % (name)
        classifier.load_rules()
        tmprst = use_classifier(classifier, testSet)
        rst = merge_rst(rst, tmprst)
        recall = sum([1 for i in rst.values() if i[consts.APP_RULE][0] or i[consts.COMPANY_RULE][0] or i[consts.CATEGORY_RULE][0]])
        print ">>> Recognized:", recall


    c, correct_app = evaluate(rst, testSet)
    correct += c
    not_cover_app = test_apps - correct_app
    recall = sum([1 for i in rst.values() if i[consts.APP_RULE].label or i[consts.COMPANY_RULE].label or i[consts.CATEGORY_RULE].label])
    print '[TEST] recall:', recall
    print '[TEST] correct:', correct
    print "Discoered App Number:", len(correct_app), "Total Number of App", len(test_apps)
    inforTrack[consts.DISCOVERED_APP] += len(correct_app) * 1.0 / len(test_apps)
    inforTrack[consts.PRECISION] += correct * 1.0 / recall
    inforTrack[consts.RECALL] += recall * 1.0 / len(testSet) * 1.0
    return rst



def cross_batch_test(train_tbls, test_tbl, appType):
    def keep_exp_app(package):
      return package.app in expApp[appType]
    expApp = load_exp_app()
    records = {}
    for tbl in train_tbls:
      records[tbl] = load_pkgs(limit = LIMIT, filterFunc = keep_exp_app, DB = tbl, appType = appType)
    
    apps = set()  
    for pkgs in records.values():
        for pkg in pkgs:
            apps.add(pkg.app)
    print "len of app", len(apps), "len of train set", len(records)

    precision = 0
    recall = 0
    discoveried_app = 0

    set_pair = []
    testSet = {record.id:record for record in load_pkgs(limit =LIMIT, filterFunc = keep_exp_app , DB = test_tbl, appType =appType)}
    set_pair.append((records, testSet))

    apps = set()
    for k,v in testSet.iteritems():
        apps.add(v.app)
    print "len of apps", len(apps), "len of test set", len(testSet)

    inforTrack = { consts.DISCOVERED_APP : 0.0, consts.PRECISION : 0.0, consts.RECALL : 0.0}

    for trainSet, testSet in set_pair:
        correct = 0
        rst = execute(trainSet, testSet, inforTrack,appType)
        if INSERT:
          print "INSERTING"
          insert_rst(rst, test_tbl)

    precision = inforTrack[consts.PRECISION]
    recall = inforTrack[consts.RECALL]
    app_coverage = inforTrack[consts.DISCOVERED_APP]
    f1_score = 2.0 * precision * recall / (precision + recall)
    print 'Precision:', precision, 'Recall:', recall, 'App:', app_coverage, 'F1 Score:', f1_score
    return 'Precision %s, Recall: %s, App: %s, F1 Score: %s' % (precision, recall, app_coverage, f1_score)
######### START ###########

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('-t', metavar='cross/single', help='test type')
    parser.add_argument('-train', metavar='tablename', nargs='+', help='train set')
    parser.add_argument('-test', metavar='tablename', help='test set')
    parser.add_argument('-apptype', metavar='apptype', help='test apptype')
    args = parser.parse_args()

    test_tbl = None
    if args.t == 'cross':
      if args.apptype.lower() == 'ios':
        appType = consts.IOS
      elif args.apptype.lower() == 'android':
        appType = consts.ANDROID
      cross_batch_test(args.train, args.test, appType)
