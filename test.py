from __future__ import absolute_import, division, print_function, unicode_literals
import time
from collections import defaultdict

import utils
from const import conf
from const.dataset import DataSetFactory
from run import train_test
from run import train
import run
import const.consts as consts
import sys

tbls = []
if conf.mode == 'l':
    tbls = ['ios_packages_2015_09_14', 'ios_packages_2015_08_10']
else:
    if conf.region == "us":
        tbls = ['ios_packages_2015_09_14', 'ios_packages_2015_08_10', 'ios_packages_2015_06_08', 'ios_packages_2015_08_12',
         'ios_packages_2015_08_04', 'ios_packages_2015_10_16','ios_packages_2015_10_21']
    elif conf.region == "cn":
        tbls = ['chi_ios_packages_2015_07_20','chi_ios_packages_2015_09_24','chi_ios_packages_2015_12_15']
    elif conf.region == "ca":
        tbls = ['ca_ios_packages_2015_12_10', 'ca_ios_packages_2015_05_29', 'ca_ios_packages_2016_02_22']
    elif conf.region == "all":
        tbls = ['ios_packages_2015_08_04', 'ios_packages_2015_10_16','ios_packages_2015_10_21',
                'ca_ios_packages_2015_12_10', 'ca_ios_packages_2015_05_29', 'ca_ios_packages_2016_02_22',
                'chi_ios_packages_2015_07_20','chi_ios_packages_2015_09_24','chi_ios_packages_2015_12_15']
    elif conf.region == "android":
        tbls = ['packages_20150210', 'packages_20150429','packages_20150509','packages_20150526','packages_20150616']


def log(trainTbls, testTbl, output):
    import time
    dateStr = str(time.strftime("%d/%m/%Y"))
    timeStr = str(time.strftime("%H:%M:%S"))
    fw = open('autotest_ios.txt', 'a')
    fw.write(dateStr + ' ' + timeStr + '\n')
    fw.write(str(trainTbls) + ' ' + testTbl + '\n')
    fw.write(str(run.USED_CLASSIFIERS) + '\n')
    fw.write(output + '\n')
    fw.write('=' * 20 + '\n')
    fw.close()


def test(testTbl):
    trainTbls = []
    for tbl in tbls:
        if tbl != testTbl:
            trainTbls.append(tbl)

    print(trainTbls, testTbl)
    inforTrack = train_test(trainTbls, testTbl, consts.IOS, ifTrain=False)
    output = _output_rst(inforTrack)
    log(trainTbls, testTbl, output)
    _compare_rst(inforTrack[consts.DISCOVERED_APP_LIST], inforTrack[consts.RESULT])
    output_app_list(inforTrack[consts.DISCOVERED_APP_LIST])


def output_app_list(discoveriedApps):
    fw = open('discovered_apps.txt', 'w')
    for appNid in discoveriedApps:
        app, trackId = appNid
        fw.write('[%s.pcap] Correct_Detection\n' % trackId)
    fw.close()
    print('Discovered Apps: discovered_apps.txt')


def _compare_rst(discoveriedApps, rst):
    """
    Input:
    - rst {pkgId: {labelType: Prediction(label, score, evidence)}}
    """
    testDisApps = set()
    for ln in open('ios_rules/discovered_apps.txt'):
        if 'Correct_Detection' in ln:
            appId = ln.strip().split('.')[0].replace('[', '')
            testDisApps.add(appId)
    print('###' * 10)
    for appid in testDisApps:
        print('test dis app ', appid)
    print('###' * 10)
    notDiscoveredPkg = []
    notDiscoveredApps = {}
    for appNid in discoveriedApps:
        app, trackId = appNid
        if trackId not in testDisApps:
            notDiscoveredApps[app] = trackId

    print('###' * 10)
    for appid in notDiscoveredApps.values():
        print('not dis appid', appid)
    print('###' * 10)

    for pkgID, predictions in rst.iteritems():
        prediction = predictions[consts.APP_RULE]
        if prediction.label in notDiscoveredApps:
            notDiscoveredPkg.append((prediction.label, notDiscoveredApps[prediction.label], str(prediction.evidence)))
    notDiscoveredPkg = sorted(notDiscoveredPkg, key=lambda x: x[0])
    for diff in notDiscoveredPkg:
        print(diff)


def _output_rst(inforTrack):
    precision = inforTrack[consts.PRECISION]
    recall = inforTrack[consts.RECALL]
    appCoverage = inforTrack[consts.DISCOVERED_APP]
    f1Score = inforTrack[consts.F1SCORE]
    instance_precision = inforTrack[consts.INSTANCE_PRECISION]
    instance_recall = inforTrack[consts.INSTANCE_RECALL]
    FPR = inforTrack['FPR']
    return 'Precision %s, Recall: %s, App: %s, F1 Score: %s InstancePrecision: %s InstanceRecall: %s FPR: %s' % \
           (precision, recall, appCoverage, f1Score, instance_precision, instance_recall, FPR)


def auto_test2():
    for path_scoreT in range(1,10,2):
        for path_labelT in range(1,10,2):
            conf.path_labelT = path_labelT / 10.0
            conf.path_scoreT = path_scoreT / 10.0
            totalPrecision = []
            totalRecall = []
            instancePrecisions = []
            instanceRecalls = []

            for testTbl in tbls:
                if testTbl != 'ios_packages_2015_08_12':
                    continue

                trainTbls = []
                for tbl in tbls:
                    if tbl != testTbl:
                        trainTbls.append(tbl)

                print(trainTbls, testTbl, conf.path_labelT, conf.path_scoreT)
                inforTrack = train_test(trainTbls, testTbl, consts.IOS, True)
                totalPrecision.append(inforTrack[consts.PRECISION])
                totalRecall.append(inforTrack[consts.RECALL])
                instancePrecisions.append(inforTrack[consts.INSTANCE_PRECISION])
                instanceRecalls.append(inforTrack[consts.INSTANCE_RECALL])
                output = _output_rst(inforTrack)
                log(trainTbls, testTbl, output)

            recall = sum(totalRecall) * 1.0 / len(totalRecall)
            instanceRecall = sum(instanceRecalls) * 1.0 / len(instanceRecalls)
            precision = sum(totalPrecision) * 1.0 / len(totalPrecision)
            instancePrecision = sum(instancePrecisions) * 1.0 / len(instancePrecisions)
            f1Score = 2.0 * precision * recall / (precision + recall)
            output = "# Precision : %s Recall: %s F1: %s InstanceP: %s InstanceR: %s Score: %s LabelT: %s" % \
                     (precision, recall, f1Score, instancePrecision, instanceRecall, conf.path_scoreT, conf.path_labelT)
            log([], '', output)

def test_combine():
    run.USED_CLASSIFIERS = [
        consts.AGENT_CLASSIFIER,
        consts.KV_CLASSIFIER,
    ]
    auto_test()
    run.USED_CLASSIFIERS = [
        consts.AGENT_CLASSIFIER,
        consts.URI_CLASSIFIER,
    ]
    auto_test()
    run.USED_CLASSIFIERS = [
        # consts.HEAD_CLASSIFIER,
        consts.KV_CLASSIFIER,
        consts.AGENT_CLASSIFIER,
        #consts.URI_CLASSIFIER,
    ]
    auto_test()
    run.USED_CLASSIFIERS = [
        # consts.HEAD_CLASSIFIER,
        consts.URI_CLASSIFIER,
        consts.KV_CLASSIFIER,
        #consts.AGENT_CLASSIFIER,
    ]
    auto_test()

def enumerate_conf():
    test_rate = [0.22, 0.18, 0.14, 0.10, 0.06, 0.02, 0]
    for i in test_rate:
        conf.agent_conf = i
        print("Agent Confidence: " + str(i))
        yield ("Agent Confidence: " + str(i))

def enumerate_support():
    test_rate = [5000, 3000, 700, 300, 89, 50, 10]
    for i in test_rate:
        conf.agent_conf = i
        yield ("Agent Support: " + str(i))

def auto_test():
    totalPrecision = []
    totalRecall = []
    instancePrecisions = []
    instanceRecalls = []
    FPRs = []

    for i in enumerate_conf():
        for testTbl in tbls:
            if testTbl == 'ios_packages_2015_06_08':
                continue

            trainTbls = []
            for tbl in tbls:
                if tbl != testTbl:
                    trainTbls.append(tbl)

            print(trainTbls, [testTbl], conf.agent_support) #, conf.path_labelT
            inforTrack = train_test(trainTbls, [testTbl], consts.IOS, ifRoc=False, ifTrain=True)
            totalPrecision.append(inforTrack[consts.PRECISION])
            totalRecall.append(inforTrack[consts.RECALL])
            instancePrecisions.append(inforTrack[consts.INSTANCE_PRECISION])
            instanceRecalls.append(inforTrack[consts.INSTANCE_RECALL])
            FPRs.append(inforTrack['FPR'])
            output = _output_rst(inforTrack)
            log(trainTbls, testTbl, output)

        recall = sum(totalRecall) * 1.0 / len(totalRecall)
        instanceRecall = sum(instanceRecalls) * 1.0 / len(instanceRecalls)
        precision = sum(totalPrecision) * 1.0 / len(totalPrecision)
        instancePrecision = sum(instancePrecisions) * 1.0 / len(instancePrecisions)
        FPR = sum(FPRs) * 1.0 / len(FPRs)
        f1Score = 2.0 * precision * recall / (precision + recall)
        output = "# Precision : %s Recall: %s F1: %s InstanceP: %s InstanceR: %s Score: %s LabelT: %s FPR: %s" % \
                 (precision, recall, f1Score, instancePrecision, instanceRecall, conf.agent_support, conf.agent_support, FPR)
        log([], '', output)

def draw_roc(fileName):
    trainTbls = ['ios_packages_2015_08_04', 'ios_packages_2015_10_16',
                'ca_ios_packages_2015_12_10', 'ca_ios_packages_2015_05_29',
                'chi_ios_packages_2015_07_20','chi_ios_packages_2015_09_24',]
    testTbls = ['ios_packages_2015_10_21', 'ca_ios_packages_2016_02_22', 'chi_ios_packages_2015_12_15']
    print(trainTbls, testTbls, 'Agent Support', conf.agent_support, 'Agent Confidence', conf.agent_conf)
    utils.clean_rules()
    trainSet = DataSetFactory.get_traindata(tbls=trainTbls, appType=consts.IOS)
    trainSet.set_label(consts.APP_RULE)
    train(trainSet, consts.IOS)
    inforTrack = train_test(trainTbls, testTbls, consts.IOS, ifTrain=False, ifRoc=True)
    roc = inforTrack['ROC']
    fw = file(fileName + '_ROC', 'w')
    for (FPR, TPR, PPV) in roc.values():
        fw.write(str(FPR)+','+str(TPR)+'\n')
    fw.close()

def test_agent():
    def enumerate_K():
        test_rate = [1, 2, 3, 4, 5]
        conf.agent_score = 0
        conf.agent_support = 10
        for i in test_rate:
            conf.agent_K = i
            print("Agent K: " + str(i))
            yield ("Agent K: " + str(i))

    def enumerate_Sup():
        test_rate = [2, 10, 30, 50, 70]
        conf.agent_K = 1
        conf.agent_score = 0
        for i in test_rate:
            conf.agent_support = i
            print("Agent Support: " + str(i))
            yield ("Agent Support: " + str(i))

    def enumerate_Score():
        test_rate = [0, 0.1, 0.3, 0.5, 0.7, 0.9]
        conf.agent_support = 30
        conf.agent_K = 1
        for i in test_rate:
            conf.agent_score = i
            print("Agent Score: " + str(i))
            yield ("Agent Score: " + str(i))
    enuFuncs = [enumerate_K, enumerate_Sup, enumerate_Score]


    for func in enuFuncs:
        totalPrecision, totalRecall, instancePrecisions, instanceRecalls, FPRs  = [], [], [], [], []
        for i in func():
            for testTbl in tbls:
                if testTbl != 'ios_packages_2015_10_21':
                    continue

                trainTbls = [tbl for tbl in tbls if tbl != testTbl]

                print(trainTbls, [testTbl], conf.agent_score, conf.agent_support, conf.agent_K)
                local_stat(FPRs, instancePrecisions, instanceRecalls, testTbl, totalPrecision, totalRecall, trainTbls)

            FPR, f1Score, instancePrecision, instanceRecall, precision, recall = global_stat(FPRs, instancePrecisions,
                                                                                             instanceRecalls,
                                                                                             totalPrecision,
                                                                                             totalRecall)
            output = "# Precision : %s Recall: %s F1: %s InstanceP: %s InstanceR: %s Score: %s LabelT: %s K: %s FPR: %s" % \
                     (precision, recall, f1Score, instancePrecision, instanceRecall, conf.agent_score, conf.agent_support,
                      conf.agent_K, FPR)
            log([], '', output)

def test_path():
    def enumerate_K():
        test_rate = [1, 2, 3, 4, 5]
        for i in test_rate:
            conf.path_K = i
            print("Path K: " + str(i))
            yield ("Path K: " + str(i))
    def enumerate_Sup():
        test_rate = [0.1, 0.3, 0.5, 0.7, 0.9]
        for i in test_rate:
            conf.path_labelT = i
            print("Path Support: " + str(i))
            yield ("Path Support: " + str(i))
    def enumerate_Score():
        test_rate = [0.1, 0.3, 0.5, 0.7, 0.9]
        for i in test_rate:
            conf.path_scoreT = i
            print("Path Score: " + str(i))
            yield ("Path Score: " + str(i))
    enuFuncs = [enumerate_K, enumerate_Sup, enumerate_Score]


    for func in enuFuncs:
        totalPrecision, totalRecall, instancePrecisions, instanceRecalls, FPRs  = [], [], [], [], []
        for i in func():
            for testTbl in tbls:
                if testTbl == 'ios_packages_2015_06_08':
                    continue

                trainTbls = [tbl for tbl in tbls if tbl != testTbl]

                print(trainTbls, [testTbl], conf.query_scoreT, conf.query_labelT, conf.query_K)
                local_stat(FPRs, instancePrecisions, instanceRecalls, testTbl, totalPrecision, totalRecall, trainTbls)

            FPR, f1Score, instancePrecision, instanceRecall, precision, recall = global_stat(FPRs, instancePrecisions,
                                                                                             instanceRecalls,
                                                                                             totalPrecision,
                                                                                             totalRecall)
            output = "# Precision : %s Recall: %s F1: %s InstanceP: %s InstanceR: %s Score: %s LabelT: %s K: %s FPR: %s" % \
                     (precision, recall, f1Score, instancePrecision, instanceRecall, conf.path_scoreT, conf.path_labelT,
                      conf.path_K, FPR)
            log([], '', output)

def test_head():
    def enumerate_K():
        test_rate = [1, 2, 3, 4, 5]
        conf.head_labelT = 0.1
        conf.head_scoreT = 0.1
        for i in test_rate:
            conf.head_K = i
            print("Head K: " + str(i))
            yield ("Head K: " + str(i))
    def enumerate_Sup():
        test_rate = [0.1, 0.3, 0.5, 0.7, 0.9]
        conf.head_K = 1
        conf.head_scoreT = 0.1
        for i in test_rate:
            conf.head_labelT = i
            print("Head Support: " + str(i))
            yield ("Head Support: " + str(i))
    def enumerate_Score():
        test_rate = [0.1, 0.3, 0.5, 0.7, 0.9]
        conf.head_labelT = 0.1
        conf.head_K = 1
        for i in test_rate:
            conf.head_scoreT = i
            print("Head Score: " + str(i))
            yield ("Head Score: " + str(i))
    enuFuncs = [enumerate_K, enumerate_Sup, enumerate_Score]


    for func in enuFuncs:
        totalPrecision, totalRecall, instancePrecisions, instanceRecalls, FPRs  = [], [], [], [], []
        for i in func():
            for testTbl in tbls:
                if testTbl != 'ios_packages_2015_08_04':
                    continue

                trainTbls = [tbl for tbl in tbls if tbl != testTbl]

                print(trainTbls, [testTbl], conf.head_scoreT, conf.head_labelT, conf.head_K)
                local_stat(FPRs, instancePrecisions, instanceRecalls, testTbl, totalPrecision, totalRecall, trainTbls)

            FPR, f1Score, instancePrecision, instanceRecall, precision, recall = global_stat(FPRs, instancePrecisions,
                                                                                             instanceRecalls,
                                                                                             totalPrecision,
                                                                                             totalRecall)
            output = "# Precision : %s Recall: %s F1: %s InstanceP: %s InstanceR: %s Score: %s LabelT: %s K: %s FPR: %s" % \
                     (precision, recall, f1Score, instancePrecision, instanceRecall, conf.head_scoreT, conf.head_labelT,
                      conf.head_K, FPR)
            log([], '', output)
def test_kv():
    def enumerate_K():
        test_rate = [1, 2, 3, 4, 5]
        for i in test_rate:
            conf.query_K = i
            print("Query K: " + str(i))
            yield ("Query K: " + str(i))
    def enumerate_Sup():
        test_rate = [0.1, 0.3, 0.5, 0.7, 0.9]
        for i in test_rate:
            conf.query_labelT = i
            print("Query Support: " + str(i))
            yield ("Query Support: " + str(i))
    def enumerate_Score():
        test_rate = [0.1, 0.3, 0.5, 0.7, 0.9]
        for i in test_rate:
            conf.query_scoreT = i
            print("Query Score: " + str(i))
            yield ("Query Score: " + str(i))
    enuFuncs = [enumerate_K, enumerate_Sup, enumerate_Sup]


    for func in enuFuncs:
        totalPrecision, totalRecall, instancePrecisions, instanceRecalls, FPRs  = [], [], [], [], []
        for i in func():
            for testTbl in tbls:
                if testTbl == 'ios_packages_2015_06_08':
                    continue

                trainTbls = [tbl for tbl in tbls if tbl != testTbl]

                print(trainTbls, [testTbl], conf.query_scoreT, conf.query_labelT, conf.query_K)
                local_stat(FPRs, instancePrecisions, instanceRecalls, testTbl, totalPrecision, totalRecall, trainTbls)

            FPR, f1Score, instancePrecision, instanceRecall, precision, recall = global_stat(FPRs, instancePrecisions,
                                                                                             instanceRecalls,
                                                                                             totalPrecision,
                                                                                             totalRecall)
            output = "# Precision : %s Recall: %s F1: %s InstanceP: %s InstanceR: %s Score: %s LabelT: %s K: %s FPR: %s" % \
                     (precision, recall, f1Score, instancePrecision, instanceRecall, conf.query_scoreT, conf.query_labelT,
                      conf.query_K, FPR)
            log([], '', output)


def global_stat(FPRs, instancePrecisions, instanceRecalls, totalPrecision, totalRecall):
    recall = sum(totalRecall) * 1.0 / len(totalRecall)
    instanceRecall = sum(instanceRecalls) * 1.0 / len(instanceRecalls)
    precision = sum(totalPrecision) * 1.0 / len(totalPrecision)
    instancePrecision = sum(instancePrecisions) * 1.0 / len(instancePrecisions)
    FPR = sum(FPRs) * 1.0 / len(FPRs)
    f1Score = 2.0 * precision * recall / (precision + recall)
    return FPR, f1Score, instancePrecision, instanceRecall, precision, recall


def local_stat(FPRs, instancePrecisions, instanceRecalls, testTbl, totalPrecision, totalRecall, trainTbls):
    inforTrack = train_test(trainTbls, [testTbl], consts.IOS, ifRoc=False, ifTrain=True)
    totalPrecision.append(inforTrack[consts.PRECISION])
    totalRecall.append(inforTrack[consts.RECALL])
    instancePrecisions.append(inforTrack[consts.INSTANCE_PRECISION])
    instanceRecalls.append(inforTrack[consts.INSTANCE_RECALL])
    FPRs.append(inforTrack['FPR'])
    output = _output_rst(inforTrack)
    log(trainTbls, testTbl, output)


def gen_rules():
    print('>>> Start training', str(time.strftime("%H:%M:%S")))
    utils.clean_rules()
    trainSet = DataSetFactory.get_traindata(tbls=tbls, appType=consts.IOS)
    trainSet.set_label(consts.APP_RULE)
    train(trainSet, consts.IOS)
    log(tbls, 'NO TEST', 'PURE TRAIN')
    print('>>> Start training', str(time.strftime("%H:%M:%S")))


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('python test.py [auto|gen|test|roc]')
    elif sys.argv[1] == 'combine':
        test_combine()
    elif sys.argv[1] == 'gen':
        gen_rules()
    elif sys.argv[1] == 'test':
        test(sys.argv[2])
    elif sys.argv[1] == 'auto':
        auto_test()
    elif sys.argv[1] == 'kv':
        test_kv()
    elif sys.argv[1] == 'path':
        test_path()
    elif sys.argv[1] == 'agent':
        test_agent()
    elif sys.argv[1] == 'head':
        test_head()
    elif sys.argv[1] == 'roc':
        if sys.argv[2] == 'agentconf':
            draw_roc('agentconf')
        elif sys.argv[2] == 'agentsupport':
            draw_roc('agentsupport')
