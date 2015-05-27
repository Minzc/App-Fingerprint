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
    for id, record in test_set.items():
        # predict
        labelDists = classifier.classify(record)
        max_confidence = -1
        for labelType, labelDist in labelDists.iteritems():
            if labelDist and filter_label_type(labelType):
                labelDist.sort(key=lambda v: v[1], reverse=True)
                if labelDist[0][1] > max_confidence:
                  rst[id] = labelDist[0][0]
    return rst


def insert_rst(rst, DB = 'packages'):
    QUERY = 'UPDATE ' + DB + ' SET classified = %s WHERE id = %s'
    print QUERY
    sqldao = SqlDao()
    for k, v in rst.items():
        sqldao.execute(QUERY,  (3, k))
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
            "Header Rule" : HeaderClassifier(),
            "CMAR Rule" : CMAR(),
            "KV RUle" : KVClassifier()
            }

    for name, classifier in classifiers.items():
        print ">>> [%s] " % (name)
        classifier.train(test_set.values())
        tmprst = use_classifier(classifier, test_set)
        rst = merge_rst(rst, tmprst)

        print ">>> Recognized:", len(rst)


    c, correct_app = evaluate(rst, test_set)
    correct += c
    inforTrack.discoveried_app += len(correct_app) * 1.0 / len(test_apps)
    inforTrack.precision += correct * 1.0 / len(rst)
    inforTrack.recall += len(rst) * 1.0 / len(test_set) * 1.0
    #####################################
    #	Text Rules
    #####################################
    # print ">>> [Classifier] Text Rules"
    # hostRuler = HostApp()
    # hostRuler.train(train_set)
    # tmprst = use_classifier(hostRuler, test_set)
    # merge_rst(rst, tmprst)
    # print ">>> Recognized:", len(rst)
    #####################################
    return rst

######### START ###########


records = load_pkgs(LIMIT, DB = "packages")
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
    test_set = {record.id:record for record in load_pkgs(LIMIT, DB = "packages_2000")}
    set_pair.append((records, test_set))

InfoTrack = namedtuple('InfoTrack', 'discoveried_app, precision, recall')
inforTrack = InfoTrack(0.0,0.0,0.0)

for train_set, test_set in set_pair:
    rnd += 1
    correct = 0
    print 'ROUND', rnd

    rst = {}

    if not DEBUG : 
        for i in train_set:
            fw.write(str(i.id)+'\n')
    rst = execute(train_set, test_set, inforTrack)


    if DEBUG:
        for i in train:
          rst[records[i].id] = 0

    # print len(rst), len(train)

    insert_rst(rst, 'packages_2000')
    if fw : fw.close()
    if DEBUG: break


print 'Precision:', inforTrack.precision / (1.0 * FOLD), 'Recall:', inforTrack.recall / (1.0 * FOLD), 'App:', inforTrack.discoveried_app / (1.0 * FOLD)
