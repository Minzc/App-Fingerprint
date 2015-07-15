from sklearn.cross_validation import KFold
import datetime
from sqldao import SqlDao
from fp import CMAR
from utils import load_pkgs, load_appinfo
from algo import KVClassifier, ParamRules2
from classifier import HeaderClassifier
from host import HostApp, PathApp
from collections import namedtuple
import consts
import sys
import argparse
from host import HostApp
from rule_manager import RuleManager


LIMIT = None
FOLD = 1
DEBUG = False
DEBUG_CMAR = False

def load_trian(size):
    train_set = {int(item.strip()) for item in open('train_id')}
    test_set = {i for i in range(size) if i not in train_set}
    return train_set, test_set

def merge_rst(rst, tmprst):
    for r in tmprst.keys():
        if r not in rst or ( rst[r] == None  and tmprst[r] != None ):
            rst[r] = tmprst[r]
    return rst


def evaluate(rst, test_set):
    # app_rst, record_id
    appCompany, appName = load_appinfo()
    correct, wrong = 0, 0
    correct_app = set()
    for k, v in rst.items():
        if v == test_set[k].app:
          correct += 1
          correct_app.add(test_set[k].app)
        elif v == appCompany[test_set[k].app]:
          correct += 1
          correct_app.add(test_set[k].app)
        else:
          wrong += 1
    print 'Total:', len(test_set), 'Recognized:', len(rst), 'Correct:', correct, 'Wrong:', wrong
    return correct, correct_app


def filter_label_type(label_type):
  return label_type == consts.APP_RULE # or label_type == consts.COMPANY_RULE

def use_classifier(classifier, test_set):
    rst = {}
    total = 0
    recall = 0
    for pkg_id, record in test_set.items():
        if len(record.queries) > 0:
          total += 1
        # predict
        labelDists = classifier.classify(record)
        max_confidence = -1
        for labelType, labelDist in labelDists.iteritems():
            if labelDist and filter_label_type(labelType):
                recall += 1
                labelDist.sort(key=lambda v: v[1], reverse=True)
                #rst[id] = labelDist[0][0]

                if labelDist[0][1] > max_confidence:
                  rst[pkg_id] = labelDist[0][0]
    
    return rst


def insert_rst(rst, DB = 'packages'):
    QUERY = 'UPDATE ' + DB + ' SET classified = %s WHERE id = %s'
    print QUERY
    sqldao = SqlDao()
    params = []
    for k, v in rst.items():
      params.append((3,k));
    sqldao.executeBatch(QUERY,  params)
    sqldao.close()
    print 'insert', len(rst),"items"



def execute(train_set, test_set, inforTrack):
    print "Train:", train_set.keys(), "Test:", len(test_set)
    correct = 0
    test_apps = set()
    rst = {}
    for record in test_set.values():
        test_apps.add(record.app)

    classifiers = [
             ("Header Rule", HeaderClassifier()),
             ("Host Rule", HostApp()),
             ("CMAR Rule", CMAR(min_cover = 3)),
             #("Path Rule" , PathApp()),
             ("KV Rule", KVClassifier())
            ]

    
    # for name, classifier in classifiers:
    #     # print ">>> [%s] " % (name)
    #     classifier =  classifier.train(train_set)
    #     classifier.loadRules()
    #     tmprst = use_classifier(classifier, test_set)
    #     rst = merge_rst(rst, tmprst)

    #    print ">>> Recognized:", len(rst)
    
    trained_classifiers = []
    ruleDict = {}
    classifierDict = {}
    for name, classifier in classifiers:
        print ">>> [%s] " % (name)
        classifier =  classifier.train(train_set)
        trained_classifiers.append((name, classifier))
        classifier.loadRules()
        ruleDict[name] = classifier.rules
        classifierDict[name] = classifier
    train_set = None # To release memory
    # hostClassifier = HostApp()
    # hostClassifier.loadRules()
    # paramClassifier = ParamRules2()
    # paramClassifier.loadRules()
    # ruleDict['Host Rule'] = hostClassifier.rules
    # ruleDict['KV Rule'] = paramClassifier.rules
    # trained_classifiers.append(('KV Rule', paramClassifier))
    # classifierDict['KV Rule'] = paramClassifier

    

    ruleManager = RuleManager()
    print '>>> Finish training all classifiers'
    print '>>> Start rule pruning'

    #classifierDict["CMAR Rule"].rules = ruleManager.pruneCMARRules(ruleDict['CMAR Rule'], ruleDict['Host Rule'])
    #classifierDict["KV Rule"].rules = ruleManager.pruneKVRules(ruleDict['KV Rule'],ruleDict['Host Rule'] )
    
    #classifierDict["CMAR Rule"].persist()
    #classifierDict["KV Rule"].persist()
    
    for name, classifier in trained_classifiers:
        print ">>> [%s] " % (name)
        classifier.loadRules()
        tmprst = use_classifier(classifier, test_set)
        rst = merge_rst(rst, tmprst)
        print ">>> Recognized:", len(rst)


    c, correct_app = evaluate(rst, test_set)
    correct += c
    not_cover_app = test_apps - correct_app
    print "Discoered App Number:", len(correct_app), "Total Number of App", len(test_apps)
    inforTrack['discoveried_app'] += len(correct_app) * 1.0 / len(test_apps)
    inforTrack['precision'] += correct * 1.0 / len(rst)
    inforTrack['recall'] += len(rst) * 1.0 / len(test_set) * 1.0
    return rst

def loadExpApp():
    expApp=set()
    for app in open("resource/exp_app.txt"):
        expApp.add(app.strip().lower())
    return expApp

def single_batch_test(train_tbl, test_tbl):
    FOLD = 5
    expApp = loadExpApp()
    records = {}
    records[tbl] = load_pkgs(LIMIT, filterFunc = lambda x: x.app in expApp , DB = train_tbl)
    
    apps = set()  
    for pkgs in records.values():
        for pkg in pkgs:
            apps.add(pkg.app)
    print "len of app", len(apps), "len of train set", len(records)

    rnd = 0

    precision = 0
    recall = 0
    discoveried_app = 0
    fw = None
    if not DEBUG:
        fw = open("train_id", "w")

    set_pair = []
    if FOLD != 1:
        if not DEBUG:
            kf = KFold(len(records), n_folds=FOLD, shuffle=True)
            for train, test in kf:
                train_set = []
                test_set = {}
                for i in train:
                    if not DEBUG : fw.write(str(i)+'\n')
                    train_set.append(records[i])
                for i in test:
                    test_set[records[i].id] = records[i]
                set_pair.append(train_set, test_set)
        else:
            sqldao = SqlDao()
            sqldao.execute('update packages set classified = NULL')
            sqldao.commit()
            sqldao.close()
            train, test = load_trian(len(records))
    else:
        test_set = {record.id:record for record in load_pkgs(LIMIT, filterFunc = lambda x: x.app in expApp , DB = args.test)}
        set_pair.append((records, test_set))
        apps = set()
        for k,v in test_set.iteritems():
            apps.add(v.app)
        print "len of apps", len(apps), "len of test set", len(test_set)

    inforTrack = { 'discoveried_app':0.0, 'precision':0.0, 'recall':0.0}


    for train_set, test_set in set_pair:
        rnd += 1
        correct = 0
        print 'ROUND', rnd

        rst = {}

        if not DEBUG : 
            for ies in train_set.values():
              for i in ies:
                fw.write(str(i.id)+'\n')
        rst = execute(train_set, test_set, inforTrack)


        if DEBUG:
            for i in train:
              rst[records[i].id] = 0

        # print len(rst), len(train)

        print "INSERTING"
        insert_rst(rst, test_tbl)
        if fw : fw.close()
        if DEBUG: break


    print 'Precision:', inforTrack['precision'] / (1.0 * FOLD), 'Recall:', inforTrack['recall'] / (1.0 * FOLD), 'App:', inforTrack['discoveried_app'] / (1.0 * FOLD)

def cross_batch_test(train_tbls, test_tbl):
    expApp = loadExpApp()
    records = {}
    for tbl in train_tbls:
       records[tbl] = load_pkgs(LIMIT, filterFunc = lambda x: x.app in expApp , DB = tbl)
    
    apps = set()  
    for pkgs in records.values():
        for pkg in pkgs:
            apps.add(pkg.app)
    print "len of app", len(apps), "len of train set", len(records)

    precision = 0
    recall = 0
    discoveried_app = 0

    set_pair = []
    test_set = {record.id:record for record in load_pkgs(LIMIT, filterFunc = lambda x: x.app in expApp , DB = test_tbl)}
    set_pair.append((records, test_set))

    apps = set()
    for k,v in test_set.iteritems():
        apps.add(v.app)
    print "len of apps", len(apps), "len of test set", len(test_set)

    inforTrack = { 'discoveried_app':0.0, 'precision':0.0, 'recall':0.0}

    for train_set, test_set in set_pair:
        correct = 0
        rst = execute(train_set, test_set, inforTrack)
        print "INSERTING"
        insert_rst(rst, test_tbl)

    precision = inforTrack['precision'] / (1.0 * FOLD)
    recall = inforTrack['recall'] / (1.0 * FOLD)
    app_coverage = inforTrack['discoveried_app'] / (1.0 * FOLD)
    f1_score = 2.0 * precision * recall / (precision + recall)
    print 'Precision:', precision, 'Recall:', recall, 'App:', app_coverage, 'F1 Score:', f1_score
    return 'Precision %s, Recall: %s, App: %s, F1 Score: %s' % (precision, recall, app_coverage, f1_score)
######### START ###########

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('-t', metavar='cross/single', help='test type')
    parser.add_argument('-train', metavar='tablename', nargs='+', help='train set')
    parser.add_argument('-test', metavar='tablename', help='test set')
    args = parser.parse_args()

    test_tbl = None
    if args.t == 'cross':
      cross_batch_test(args.train, args.test)
    elif args.t == 'single':
      single_batch_test(args.train, args.train)
