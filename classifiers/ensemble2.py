from collections import defaultdict
from const import conf
from classifiers.classifier_factory import classifier_factory
from const import consts
from const.dataset import DataSetFactory, DataSetIter
from run import USED_CLASSIFIERS, _classify
from sqldao import SqlDao

TRAIN_LABEL = consts.APP_RULE
trainTbls = ['ios_packages_2015_08_04', 'ios_packages_2015_10_16']
testTbl = ['ios_packages_2015_10_21']
OUTPUTS = ["ios_test.csv", "ios_train.csv"]
#trainTbls = ['packages_20150210', 'packages_20150509','packages_20150526','packages_20150616']
#testTbl = ['packages_20150429']

trainSet = DataSetFactory.get_traindata(tbls=trainTbls, appType=consts.ANDROID)
trainSet.set_label(TRAIN_LABEL)
testSet = DataSetFactory.get_traindata(tbls=testTbl, appType=consts.ANDROID)

groundT = defaultdict(set)
for _, pkg in DataSetIter.iter_pkg(testSet):
    groundT[pkg.id] = pkg.app
for _, pkg in DataSetIter.iter_pkg(trainSet):
    groundT[pkg.id] = pkg.app

print("Data Length", len(groundT))
sqldao = SqlDao()
SQL_SELECT_ALL_RULES = (' SELECT id'
                          ' FROM {}').format(conf.ruleSet)
allRuleIDs = [id for id in sqldao.execute(SQL_SELECT_ALL_RULES)]
classifiers = classifier_factory(USED_CLASSIFIERS, consts.IOS)


testApps = set()
rst = defaultdict(lambda : [None, None, None, None, None, None])
for name, classifier in classifiers:
    print ">>> [test#%s] " % name
    classifier.set_name(name)
    classifier.load_rules()
    rawP = _classify(classifier, testSet)
    for pkgid, predict in rawP.items():
        label, _, _, ruleid = predict[consts.APP_RULE]
        if label != None:
            rst[pkgid][ruleid] = 1
            rst[pkgid]["T"] = groundT[pkgid]
        testApps.add(groundT[pkgid])

print("Apps:", len(testApps))
writer = file(OUTPUTS[0], "w")
for pkgid, predicts in rst.items():
    output = ''
    for id in allRuleIDs:
        if id in predicts:
            output = output + ', 1'
        else:
            output = output + ', 0'
    output = output + ', ' + predicts['T'] + '\n'
    writer.write(output)
writer.close()

rst = defaultdict(lambda : [None, None, None, None, None, None])
for name, classifier in classifiers:
    print ">>> [test#%s] " % name
    classifier.set_name(name)
    classifier.load_rules()
    rawP = _classify(classifier, trainSet )
    for pkgid, predict in rawP.items():
        label, _, _, ruleid = predict[consts.APP_RULE]
        if label != None:
            rst[pkgid][ruleid] = 1
            rst[pkgid]["T"] = groundT[pkgid]


writer = file(OUTPUTS[1], "w")
for pkgid, predicts in rst.items():
    output = ''
    for id in allRuleIDs:
        if id in predicts:
            output = output + ', 1'
        else:
            output = output + ', 0'
    output = output + ', ' + predicts['T'] + '\n'
    writer.write(output)
writer.close()



