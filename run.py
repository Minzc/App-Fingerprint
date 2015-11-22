# -*- coding=utf-8 -*-
from sqldao import SqlDao
from utils import load_pkgs, load_exp_app
from collections import defaultdict
import const.consts as consts
import argparse
from const.dataset import DataSetFactory as DataSetFactory
from const.dataset import DataSetIter as DataSetIter
from classifiers.classifier_factory import classifier_factory

LIMIT = None
INSERT = True
PRUNE = False

VALID_LABEL = {consts.APP_RULE, consts.COMPANY_RULE, consts.CATEGORY_RULE}
TRAIN_LABEL = {
    consts.APP_RULE,
    # consts.COMPANY_RULE,
    # consts.CATEGORY_RULE
}

USED_CLASSIFIERS = [
    # consts.HEAD_CLASSIFIER,
    consts.AGENT_CLASSIFIER,
    consts.KV_CLASSIFIER,
    consts.CMAR_CLASSIFIER,
    consts.HOST_CLASSIFIER,

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
        records[tbl] = load_pkgs(limit=LIMIT, filterFunc=_keep_exp_app, DB=tbl, appType=appType)

    return records


def train(trainTbls, appType):
    """
    1. Load data from database from given tables
    2. Train classifiers, prune and persist rules in database
    Input
    :param trainTbls: A list of tables used to train classifiers
    :parm appType: android or ios
    """
    trainSet = DataSetFactory.get_traindata(tbls=trainTbls, sampleRate=1.0, appType=appType, LIMIT=LIMIT)
    for ruleType in TRAIN_LABEL:
        trainSet.set_label(ruleType)
        classifiers = classifier_factory(USED_CLASSIFIERS, appType)
        for name, classifier in classifiers:
            classifier.set_name(name)
            print ">>> [train#%s] " % name
            classifier.train(trainSet, ruleType)

    print '>>> Finish training all classifiers'


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
    inforTrack = {consts.DISCOVERED_APP: 0.0, consts.PRECISION: 0.0, consts.RECALL: 0.0}
    correct, recall = 0, 0
    correctApp = set()
    wrongApp = set()
    detectedApp = set()
    for tbl, pkg in DataSetIter.iter_pkg(testSet):
        predictions = rst[pkg.id]
        predictApp = predictions[consts.APP_RULE].label
        predictCompany = predictions[consts.COMPANY_RULE].label
        predictCategory = predictions[consts.CATEGORY_RULE].label
        ifCorrect = True
        if predictApp and predictApp != pkg.app:
            ifCorrect = False
        if predictCompany and predictCompany != pkg.company:
            ifCorrect = False
        if predictCategory and predictCategory != pkg.category:
            ifCorrect = False

        if sum([1 for value in predictions.values() if value != consts.NULLPrediction]) > 0:
            recall += 1
            detectedApp.add((pkg.app, pkg.appInfo.trackId))
            if ifCorrect:
                correct += 1
                correctApp.add((pkg.app, pkg.appInfo.trackId))
            else:
                wrongApp.add((pkg.app, pkg.appInfo.trackId))

    print '[TEST] Total:', testSet.get_size().values()[0]
    print '[TEST] Recall:', recall
    print '[TEST] Correct:', correct
    print '[TEST] Correct Number of App:', len(correctApp)
    print '[TEST] Wrong Number of App:', len(wrongApp)
    print '[TEST] Total Detect Number of App:', len(detectedApp)
    print '[TEST] Total Number of App:', len(testApps)

    precision = correct * 1.0 / recall
    recall = recall * 1.0 / testSet.get_size().values()[0]
    f1Score = 2.0 * precision * recall / (precision + recall)
    inforTrack[consts.DISCOVERED_APP] = len(correctApp.difference(wrongApp)) * 1.0 / len(testApps)
    inforTrack[consts.PRECISION] = precision
    inforTrack[consts.RECALL] = recall
    inforTrack[consts.F1SCORE] = f1Score
    inforTrack[consts.DISCOVERED_APP_LIST] = detectedApp
    return inforTrack


def _use_classifier(classifier, testSet):
    """
    Use trained classifer on the given test data set
    Input
    - classifier : classifier used on the data set
    - testSet : testData set {pkgId : package}
    """
    wrongApp = set()
    batchPredicts = classifier.classify(testSet)
    rst = defaultdict(dict)

    for pkgId, predicts in batchPredicts.items():
        for ruleType, predict in filter(lambda x: x[0] in VALID_LABEL, predicts.items()):
            rst[pkgId][ruleType] = predict

    for tbl, pkg in DataSetIter.iter_pkg(testSet):
        predict = rst[pkg.id][consts.APP_RULE]
        if predict.label is not None and predict.label != pkg.app:
            wrongApp.add(pkg.app)

    print '====', classifier.name, '====', '[WRONG]', len(wrongApp)
    return rst


def _insert_rst(testSet, DB, inforTrack):
    """
  Insert prediction results into data base
  Input
  - DB : inserted table name
  - testSet : test packages
  - infoTrack : information about test result
  """
    print 'Start inserting results'
    QUERY = 'UPDATE ' + DB + ' SET classified = %s WHERE id = %s'
    sqldao = SqlDao()
    params = []
    detectedApps = {app for app, _ in inforTrack[consts.DISCOVERED_APP_LIST]}
    for tbl, pkg in DataSetIter.iter_pkg(test()):
        if pkg.app not in detectedApps:
            params.append((3, pkg.id))

    sqldao.executeBatch(QUERY, params)
    sqldao.close()
    print 'Finish inserting %s items' % len(params)


def _clean_up():
    sqldao = SqlDao()
    sqldao.execute(consts.SQL_CLEAN_ALL_RULES)
    sqldao.close()
    print consts.SQL_CLEAN_ALL_RULES


def test(testTbl, appType):
    testSet = DataSetFactory.get_traindata(tbls=[testTbl], sampleRate=1.0, appType=appType, LIMIT=LIMIT)
    testApps = testSet.apps

    testSize = testSet.get_size()[testTbl]

    rst = {}
    classifiers = classifier_factory(USED_CLASSIFIERS, appType)
    for name, classifier in classifiers:
        print ">>> [test#%s] " % name
        classifier.set_name(name)
        classifier.load_rules()
        tmprst = _use_classifier(classifier, testSet)
        rst = merge_rst(rst, tmprst)
        recall = sum([1 for i in rst.values() if
                      i[consts.APP_RULE][0] or i[consts.COMPANY_RULE][0] or i[consts.CATEGORY_RULE][0]])
        print ">>> Recognized: %s Test Size: %s" % (recall, testSize)

    print '>>> Start evaluating'
    inforTrack = evaluate(rst, testSet, testApps)
    inforTrack[consts.RESULT] = rst
    if INSERT:
        _insert_rst(testSet, testTbl, inforTrack)
    return inforTrack


def cross_batch_test(trainTbls, testTbl, appType, ifTrain=True):
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
