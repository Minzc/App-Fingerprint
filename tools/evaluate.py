from __future__ import absolute_import, division, print_function, unicode_literals
from collections import defaultdict

from const import consts
from const.dataset import DataSetIter

def cal_roc(rst, testSet):
    P, groundT, T = defaultdict(set), defaultdict(set), defaultdict(int)

    for tbl, pkg in DataSetIter.iter_pkg(testSet):
        groundT[pkg.id] = pkg
        T[pkg.app] += 1

    scores = set()
    for pkgId, predictions in rst.items():
        if predictions[consts.APP_RULE] is not None:
            predict = predictions[consts.APP_RULE]
            if predict.label is not None:
                P[predict.label].add((pkgId, predict.score))
                scores.add(predict.score)

    total = len(groundT)
    roc = {}
    print("Start Construct ROC Curve ", len(scores))
    for scoreT in scores:
        correctApp, wrongApp, detectApp = set(), set(), set()
        accTP, accFP, accTN, accFN = 0, 0, 0, 0
        for app, tNum in T.items():
            TP, FP, TN, FN = 0, 0, 0, 0
            tmpP = [(pkgId, s) for (pkgId, s) in P[app] if s >= scoreT]
            pNum = len(tmpP)
            for pkgId, _ in tmpP:
                if app == groundT[pkgId].app:
                    TP += 1

            if TP > 0 and TP == pNum:
                correctApp.add(app)
            elif pNum > 0 and TP != pNum:
                wrongApp.add(app)
            if pNum > 0:
                detectApp.add(app)

            FP = (pNum - TP)
            FN = (tNum - TP)
            TN = ((total - tNum) - FP)

            accTP += TP
            accFP += FP
            accTN += TN
            accFN += FN

        FPR = accFP / (accFP + accTN)
        TPR = accTP / (accTP + accFN) # Recall
        PPV = accTP / (accTP + accFP) # Precision
        roc[scoreT] = (FPR, TPR, PPV)
    return roc



def __compare(rst, testSet):
    """
      Compare predictions with test data set
      Input:
      :param  rst : Predictions got from test. {pkgId : {ruleType : prediction}}
      :param  testSet : Test data set. {pkgId : pacakge}
      :param  testApps : Tested apps
      Output:
      - InforTrack : contains evaluation information
    """
    P, groundT, T = defaultdict(set), defaultdict(set), defaultdict(int)

    for tbl, pkg in DataSetIter.iter_pkg(testSet):
        groundT[pkg.id] = pkg
        T[pkg.app] += 1
    for pkgId, predictions in rst.items():
        if predictions[consts.APP_RULE] is not None:
            label = predictions[consts.APP_RULE].label
            # label, vote = None, 0
            # for l in predictions[consts.APP_RULE]:
            #     if predictions[consts.APP_RULE][l] > vote:
            #         vote = predictions[consts.APP_RULE][l]
            #         label = l
            if label is not None:
                P[label].add(pkgId)

    total = len(groundT)
    correctApp, wrongApp, detectApp = set(), set(), set()
    accTP, accFP, accTN, accFN = 0, 0, 0, 0
    for app, tNum in T.items():
        TP, FP, TN, FN = 0, 0, 0, 0
        pNum = len(P[app])
        for pkgId in P[app]:
            if app == groundT[pkgId].app:
                TP += 1

        if TP > 0 and TP == pNum:
            correctApp.add(app)
        elif pNum > 0 and TP != pNum:
            wrongApp.add(app)
        if pNum > 0:
            detectApp.add(app)

        FP = (pNum - TP)
        FN = (tNum - TP)
        TN = ((total - tNum) - FP)

        accTP += TP
        accFP += FP
        accTN += TN
        accFN += FN

    FPR = accFP / (accFP + accTN)
    TPR = accTP / (accTP + accFN) # Recall
    PPV = accTP / (accTP + accFP) # Precision
    print('Recall:', (accTP + accFN), '#', accTP)
    print('Precision:', (accTP + accFP), '#', accTP)
    return FPR, TPR, PPV, correctApp, wrongApp, detectApp

def __wrap_result(rst, FPR, TPR, PPV, correctApp, wrongApp, detectApp, testApps):
    inforTrack = {}
    precision = len(correctApp) / (len(correctApp) + len(wrongApp))
    recall = len(correctApp) / len(testApps)
    f1Score = 2.0 * precision * recall / (precision + recall)
    inforTrack[consts.DISCOVERED_APP] = len(correctApp.difference(wrongApp)) / len(testApps)
    inforTrack[consts.PRECISION] = precision
    inforTrack[consts.INSTANCE_PRECISION] = PPV
    inforTrack[consts.INSTANCE_RECALL] = TPR
    inforTrack[consts.RECALL] = recall
    inforTrack[consts.F1SCORE] = f1Score
    inforTrack[consts.DISCOVERED_APP_LIST] = detectApp
    inforTrack[consts.RESULT] = rst
    inforTrack['FPR'] = FPR
    return inforTrack

def cal_pr(rst, testSet, testApps):
    FPR, TPR, PPV, correctApp, wrongApp, detectApp = __compare(rst, testSet)
    print('[TEST] Total:', testSet.get_size().values()[0])
    print('[TEST] Recall:', TPR)
    print('[TEST] Correct:', PPV)
    print('[TEST] FPR', FPR)
    print('[TEST] Correct Number of App:', len(correctApp))
    print('[TEST] Total Detect Number of App:', len(detectApp))
    print('[TEST] Total Number of App:', len(testApps))
    inforTrack = __wrap_result(rst, FPR, TPR, PPV, correctApp, wrongApp, detectApp, testApps)
    return inforTrack