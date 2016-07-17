from collections import defaultdict

from classifiers.classifier_factory import classifier_factory
from const import consts
from const.dataset import DataSetFactory, DataSetIter
from run import USED_CLASSIFIERS, _classify

ENCODE = {
    consts.HEAD_CLASSIFIER: 1,
    consts.AGENT_BOUNDARY_CLASSIFIER: 2,
    consts.KV_CLASSIFIER: 3,
    consts.URI_CLASSIFIER: 4,
    "T": 5}

TRAIN_LABEL = consts.APP_RULE
trainTbls = ['ios_packages_2015_08_04', 'ios_packages_2015_10_21']
testTbl = ['ios_packages_2015_10_16']

#trainTbls = ['packages_20150210', 'packages_20150509','packages_20150526','packages_20150616']
#testTbl = ['packages_20150429']

trainSet = DataSetFactory.get_traindata(tbls=trainTbls, appType=consts.IOS)
trainSet.set_label(TRAIN_LABEL)
testSet = DataSetFactory.get_traindata(tbls=testTbl, appType=consts.IOS)

groundT = defaultdict(set)
for _, pkg in DataSetIter.iter_pkg(testSet):
    groundT[pkg.id] = pkg.app
for _, pkg in DataSetIter.iter_pkg(trainSet):
    groundT[pkg.id] = pkg.app

print("Data Length", len(groundT))

classifiers = classifier_factory(USED_CLASSIFIERS, consts.IOS)


apps = set()
rst = defaultdict(lambda : [None, None, None, None, None, None])
for name, classifier in classifiers:
    print ">>> [test#%s] " % name
    classifier.set_name(name)
    classifier.load_rules()
    rawP = _classify(classifier, testSet)
    for pkgid, predict in rawP.items():
        predict = predict[consts.APP_RULE]
        rst[pkgid][ENCODE[name]] = predict.label
        rst[pkgid][ENCODE["T"]] = groundT[pkgid]
        apps.add(groundT[pkgid])

print("Apps:", len(apps))
writer = file("ios_test.csv", "w")
for pkgid, predicts in rst.items():
    if predicts[5] in apps:
        if predicts[1] is not None or predicts[2] is not None or predicts[3] is not None or predicts[4] is not None:
            output = "%s, %s, %s, %s, %s\n" % (predicts[1], predicts[2], predicts[3], predicts[4], predicts[5])
            writer.write(output)
writer.close()

rst = defaultdict(lambda : [None, None, None, None, None, None])
for name, classifier in classifiers:
    print ">>> [test#%s] " % name
    classifier.set_name(name)
    classifier.load_rules()
    rawP = _classify(classifier, trainSet )
    for pkgid, predict in rawP.items():
        predict = predict[consts.APP_RULE]
        rst[pkgid][ENCODE[name]] = predict.label
        rst[pkgid][ENCODE["T"]] = groundT[pkgid]


writer = file("ios_train.csv", "w")
for pkgid, predicts in rst.items():
    if predicts[5] in apps:
        if predicts[1] is not None or predicts[2] is not None or predicts[3] is not None or predicts[4] is not None:
            output = "%s, %s, %s, %s, %s\n" % (predicts[1], predicts[2], predicts[3], predicts[4], predicts[5])
            writer.write(output)
writer.close()



