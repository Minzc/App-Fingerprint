# -*- coding=utf-8 -*-
import argparse
from collections import defaultdict

import const.conf
import const.consts as consts
import utils
from classifiers.classifier_factory import classifier_factory
from const import conf
from const.conf import INSERT
from const.dataset import DataSetFactory as DataSetFactory
from const.dataset import DataSetIter as DataSetIter
from sqldao import SqlDao

TRAIN_LABEL = consts.APP_RULE

VALID_LABEL = {
    consts.APP_RULE,
    # consts.COMPANY_RULE,
    # consts.CATEGORY_RULE
}

if not const.conf.TestBaseLine:
    USED_CLASSIFIERS = [
        # consts.HEAD_CLASSIFIER,
        consts.AGENT_CLASSIFIER,
        consts.KV_CLASSIFIER,
        consts.URI_CLASSIFIER,
    ]
else:
     USED_CLASSIFIERS = [
         consts.Agent_BL_CLASSIFIER,
         consts.Query_BL_CLASSIFIER
    ]


class PredictRst:
    def __init__(self):
        self.__correct = 0
        self.__predict = 0
        self.__appInfo = None

    def set_appInfo(self, appInfo):
        self.__appInfo = appInfo

    def inc_correct(self, n):
        self.__correct += n

    def inc_total(self):
        self.__predict += 1

    @property
    def total(self):
        return self.__predict

    @property
    def correct(self):
        return self.__correct

    @property
    def trackId(self):
        return self.__appInfo.trackId

    @property
    def package(self):
        return self.__appInfo.package

    @property
    def wrong(self):
        return self.__predict - self.__correct

    def if_all_right(self):
        return self.__predict == self.__correct and self.__predict > 0


def pipeline(rst, tmprst):
    for pkg_id, predictions in tmprst.iteritems():
        if pkg_id not in rst:
            rst[pkg_id] = {}
            for rule_type in VALID_LABEL:
                rst[pkg_id][rule_type] = {}

        for rule_type in VALID_LABEL:
            label = tmprst[pkg_id][rule_type].label
            if len(rst[pkg_id][rule_type]) == 0 and label is not None:
                rst[pkg_id][rule_type][label] = 1
    return rst

def vote_rst(rst, tmprst):
    for pkg_id, predictions in tmprst.iteritems():
        if pkg_id not in rst:
            rst[pkg_id] = {}
            for rule_type in VALID_LABEL:
                rst[pkg_id][rule_type] = {}

        for rule_type in VALID_LABEL:
            label = tmprst[pkg_id][rule_type].label
            if label is not None:
                if label not in rst[pkg_id][rule_type]:
                    rst[pkg_id][rule_type][label] = 1
                else:
                    rst[pkg_id][rule_type][label] += 1
    return rst


def train(trainSet, appType):
    """
    1. Load data from database from given tables
    2. Train classifiers, prune and persist prune in database
    Input
    :param trainSet: A list of tables used to train classifiers
    :parm appType: android or ios
    """
    classifiers = classifier_factory(USED_CLASSIFIERS, appType)
    for name, classifier in classifiers:
        classifier.set_name(name)
        print ">>> [train#%s] " % name
        classifier.train(trainSet, TRAIN_LABEL)
    print '>>> Finish training all classifiers'




def _evaluate(rst, testSet, testApps):
    """
  Compare predictions with test data set
  Input:
  :param  rst : Predictions got from test. {pkgId : {ruleType : prediction}}
  :param  testSet : Test data set. {pkgId : pacakge}
  :param  testApps : Tested apps
  Output:
  - InforTrack : contains evaluation information
  """

    # app_rst, record_id
    inforTrack = {}
    correct, recall = 0, 0
    wrongApp, correctApp, detectedApp = set(), set(), set()
    appPkgs = defaultdict(set)
    for tbl, pkg in DataSetIter.iter_pkg(testSet):
        appPkgs[pkg.app].add(pkg)

    for app, pkgs in appPkgs.items():
        cPrdcts = PredictRst()
        for pkg in pkgs:
            cPrdcts.set_appInfo(pkg.appInfo)
            predictions = rst[pkg.id]
            correctLabels = [0, 0, 0]
            ifPredict = False

            for ruleType in VALID_LABEL:
                predict = None
                if len(predictions[ruleType]) > 0:
                    ifPredict = True
                    predict = max(predictions[ruleType].items(), key=lambda x:x[1])[0]
                label = utils.get_label(pkg, ruleType)
                correctLabels[ruleType] = 1 if label == predict else 0

            if ifPredict:
                cPrdcts.inc_total()
                if sum(correctLabels) > 0:
                    cPrdcts.inc_correct(1)
                else:
                    print '[ERROR]', predictions, pkg.app

        if cPrdcts.if_all_right():
            correctApp.add((cPrdcts.package, cPrdcts.trackId))
        if cPrdcts.correct > 0:
            detectedApp.add((cPrdcts.package, cPrdcts.trackId))
        if cPrdcts.wrong > 0:
            wrongApp.add((cPrdcts.package, cPrdcts.trackId))
        correct += cPrdcts.correct
        recall += cPrdcts.total

    assert len(correctApp.intersection(wrongApp)) == 0
    print '[TEST] Total:', testSet.get_size().values()[0]
    print '[TEST] Recall:', recall
    print '[TEST] Correct:', correct
    print '[TEST] Correct Number of App:', len(correctApp)
    print '[TEST] Wrong Number of App:', len(wrongApp)
    print '[TEST] Total Detect Number of App:', len(detectedApp)
    print '[TEST] Total Number of App:', len(testApps)

    instance_precision = correct * 1.0 / recall
    instance_recall = recall * 1.0 / testSet.get_size().values()[0]
    precision = len(correctApp) * 1.0 / (len(correctApp) + len(wrongApp))
    recall = (len(correctApp) + len(wrongApp)) * 1.0 / len(testApps)
    f1Score = 2.0 * precision * recall / (precision + recall)
    inforTrack[consts.DISCOVERED_APP] = len(correctApp.difference(wrongApp)) * 1.0 / len(testApps)
    inforTrack[consts.PRECISION] = precision
    inforTrack[consts.INSTANCE_PRECISION] = instance_precision
    inforTrack[consts.INSTANCE_RECALL] = instance_recall
    inforTrack[consts.RECALL] = recall
    inforTrack[consts.F1SCORE] = f1Score
    inforTrack[consts.DISCOVERED_APP_LIST] = detectedApp
    inforTrack[consts.RESULT] = rst
    return inforTrack


def _classify(classifier, testSet):
    """
    Use trained classifer on the given test data set
    Input
    - classifier : classifier used on the data set
    - testSet : testData set {pkgId : package}
    """
    batchPredicts = classifier.classify(testSet)
    rst = defaultdict(dict)

    for pkgId, predicts in batchPredicts.items():
        for ruleType, predict in filter(lambda x: x[0] in VALID_LABEL, predicts.items()):
            rst[pkgId][ruleType] = predict
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
    sqldao = SqlDao()
    params = []
    QUERY = 'UPDATE ' + DB + ' SET classified = %s WHERE id = %s'
    detectedApps = {app for app, _ in inforTrack[consts.DISCOVERED_APP_LIST]}
    for tbl, pkg in DataSetIter.iter_pkg(testSet):
        if pkg.app not in detectedApps:
            params.append((3, pkg.id))

    sqldao.executeBatch(QUERY, params)
    sqldao.close()
    print 'Finish inserting %s items' % len(params)


def test(testTbl, appType):
    testSet = DataSetFactory.get_traindata(tbls=[testTbl], appType=appType)
    testApps = testSet.apps

    rst = {}
    rst2 = {}
    classifiers = classifier_factory(USED_CLASSIFIERS, appType)
    for name, classifier in classifiers:
        print ">>> [test#%s] " % name
        classifier.set_name(name)
        classifier.load_rules()
        tmprst = _classify(classifier, testSet)
        # if conf.ensamble == "pipeline":
        rst = pipeline(rst, tmprst)
        # elif conf.ensamble == "vote":
        rst2 = vote_rst(rst2, tmprst)

    assert len(rst2) == len(rst)
    for pkgid in rst:
        print(rst[pkgid][consts.APP_RULE])
        print(rst2[pkgid][consts.APP_RULE])
        assert None not in rst[pkgid][consts.APP_RULE]
        if len(rst[pkgid][consts.APP_RULE]) > 0:
            assert None not in rst2[pkgid][consts.APP_RULE]
            assert len(rst2[pkgid][consts.APP_RULE]) > 0


    print '>>> Start evaluating'
    inforTrack = _evaluate(rst, testSet, testApps)
    if INSERT:
        _insert_rst(testSet, testTbl, inforTrack)
    return inforTrack


def train_test(trainTbls, testTbl, appType, ifTrain=True):
    if ifTrain:
        print '>>> Start training'
        utils.clean_rules()
        trainSet = DataSetFactory.get_traindata(tbls=trainTbls, appType=appType)
        trainSet.set_label(TRAIN_LABEL)
        train(trainSet, appType)
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
        train_test(args.train, args.test, appType)
