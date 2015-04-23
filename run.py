from sklearn.cross_validation import KFold

from sqldao import SqlDao
import fp
from utils import load_pkgs


LIMIT = None


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
        if v == test_set[k].app or test_set[k].company in set(v.split('$')) or v in test_set[k].name:
            correct += 1
            correct_app.add(v)
        else:
            wrong += 1
    print 'Total:', len(test_set), 'Recognized:', len(rst), 'Correct:', correct, 'Wrong:', wrong
    return correct, correct_app


def use_classifier(classifier, test_set):
    rst = {}
    for id, record in test_set.items():
        # predict
        labelDist = classifier.classify(record)
        for label in labelDist:
            if label:
                rst[id] = label
                break
    return rst


def insert_rst(rst):
    QUERY = 'UPDATE packages SET classified = %s WHERE id = %s'
    sqldao = SqlDao()
    for k, v in rst.items():
        sqldao.execute(QUERY, (3, k))
    sqldao.close()


records = load_pkgs(LIMIT)

kf = KFold(len(records), n_folds=10, shuffle=True)

rnd = 0

precision = 0
recall = 0
discoveried_app = 0

for train, test in kf:
    rnd += 1
    correct = 0
    print 'ROUND', rnd

    train_set = []
    test_set = {}
    rst = {}
    test_apps = set()

    for i in train:
        train_set.append(records[i])
    for i in test:
        test_set[records[i].id] = records[i]
        test_apps.add(records[i].app)

    # train_set = reservoir_sample(len(train_set)*0.1, train_set)
    #####################################
    # FP Rules
    ######################################
    fpClassifier = fp.mine_fp(train_set, 2, 0.8)
    rst = use_classifier(fpClassifier, test_set)
    #####################################
    #	Text Rules
    ######################################
    #txtClassifier = app_txt_f.mine_txt(train_set)
    #rst = use_classifier(txtClassifier, test_set)
    #####################################
    # tf_idf(train_set)
    # path_algo.host_tree(train_set)
    # rst = classifier.classify(True, test_set.values())
    # correct = evaluate(rst, test_set)
    # algo.train(train_set)
    # tmprst = algo.test_algo(test_set.values())
    # rst = merge_rst(rst, tmprst)
    c, correct_app = evaluate(rst, test_set)
    correct += c
    discoveried_app += len(correct_app) * 1.0 / len(test_apps)
    precision += correct * 1.0 / len(rst)
    recall += len(rst) * 1.0 / len(test_set) * 1.0
    insert_rst(rst)

print 'Precision:', precision / 10.0, 'Recall:', recall / 10.0, 'App:', discoveried_app / 10.0

