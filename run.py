from sklearn.cross_validation import KFold
import datetime
from sqldao import SqlDao
from fp import CMAR
from utils import load_pkgs
from algo import KVClassifier
from classifier import HeaderClassifier
from host import HostApp
from collections import namedtuple
import consts
import sys
import argparse
from host import HostApp


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
        if r not in rst:
            rst[r] = tmprst[r]
    return rst


def evaluate(rst, test_set):
    # app_rst, record_id
    correct, wrong = 0, 0
    correct_app = set()
    for k, v in rst.items():
        # if v == test_set[k].app or test_set[k].company in set(v.split('$')) or v in test_set[k].name:
        if v == test_set[k].app:
            correct += 1
            correct_app.add(test_set[k].app)
        else:
            wrong += 1
    print 'Total:', len(test_set), 'Recognized:', len(rst), 'Correct:', correct, 'Wrong:', wrong
    return correct, correct_app


def filter_label_type(label_type):
  return label_type == consts.APP_RULE

def use_classifier(classifier, test_set):
    rst = {}
    total = 0
    recall = 0
    for id, record in test_set.items():
        total += 1
        # predict
        labelDists = classifier.classify(record)
        print labelDists
        max_confidence = -1
        for labelType, labelDist in labelDists.iteritems():
            recall += 1
            if labelDist and filter_label_type(labelType):
                labelDist.sort(key=lambda v: v[1], reverse=True)
                
                print 'labelDist', labelDist

                if labelDist[0][1] > max_confidence:
                  rst[id] = labelDist[0][0]
    
    print total, recall
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
    print "Train:", len(train_set), "Test:", len(test_set)
    correct = 0
    test_apps = set()
    rst = {}
    for record in test_set.values():
        test_apps.add(record.app)

    classifiers = {
             #"Header Rule" : HeaderClassifier(),
             #"CMAR Rule" : CMAR(3),
             #"Host Rule" : HostApp(),
             "KV RUle" : KVClassifier()
            }

    for name, classifier in classifiers.items():
        print ">>> [%s] " % (name)
        classifier.train(train_set)
        print 'train finish'
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
    #####################################
    # Text Rules
    #####################################
    # print ">>> [Classifier] Text Rules"
    # hostRuler = HostApp()
    # hostRuler.train(train_set)
    # tmprst = use_classifier(hostRuler, test_set)
    # merge_rst(rst, tmprst)
    # print ">>> Recognized:", len(rst)
    #####################################
    return rst

def loadExpApp():
    expApp=set()
    for app in open("resource/exp_app.txt"):
        expApp.add(app.strip().lower())
    return expApp

######### START ###########

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('-t', metavar='cross/single', help='test type')
    parser.add_argument('-train', metavar='tablename', nargs='+', help='train set')
    parser.add_argument('-test', metavar='tablename', help='test set')
    args = parser.parse_args()

    test_tbl = args.train
    if args.t == 'cross':
        FOLD = 1
        test_tbl = args.test
    elif args.t == 'single':
        FOLD = 5

    expApp = loadExpApp()
    records = {}
    for tbl in args.train:
        records[tbl] = load_pkgs(LIMIT, filterFunc = lambda x: x.app in expApp ,DB = tbl)
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
