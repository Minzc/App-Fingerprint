from run import cross_batch_test
from run import TRAIN_LABEL
from run import USED_CLASSIFIERS
from run import train
import const.consts as consts
import sys

tbls = [   'ios_packages_2015_09_14','ios_packages_2015_08_10', 'ios_packages_2015_06_08', 'ios_packages_2015_08_12', 'ios_packages_2015_08_04']
# tbls = ['ios_packages_2015_08_12', 'ios_packages_2015_08_10']


def log(trainTbls, testTbl, output):
  import time
  dateStr = str(time.strftime("%d/%m/%Y"))
  timeStr = str(time.strftime("%H:%M:%S"))
  fw = open('autotest_ios.txt', 'a')
  fw.write(dateStr + ' ' + timeStr+'\n')
  fw.write(str(trainTbls)+' ' + testTbl+'\n')
  fw.write(str(TRAIN_LABEL) + '\n')
  fw.write(str(USED_CLASSIFIERS) + '\n')
  fw.write(output + '\n')
  fw.write('=' * 20 + '\n')
  fw.close()

def test(testTbl):
  trainTbls = []
  for tbl in tbls:
    if tbl != testTbl:
      trainTbls.append(tbl)

  print trainTbls, testTbl
  inforTrack = cross_batch_test(trainTbls, testTbl, consts.IOS, ifTrain = True)
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
  print 'Discovered Apps: discovered_apps.txt'

def _compare_rst(discoveriedApps, rst):
  '''
  Input:
  - rst {pkgId: {labelType: Prediction(label, score, evidence)}}
  '''
  testDisApps = set()
  for ln in open('ios_rules/discovered_apps.txt'):
    if 'Correct_Detection' in ln:
        appId = ln.strip().split('.')[0].replace('[','')
        testDisApps.add(appId)
  print '###' * 10
  for appid in testDisApps:
    print 'test dis app ', appid
  print '###' * 10
  notDiscoveredPkg = []
  notDiscoveredApps = {}
  for appNid in discoveriedApps:
    app, trackId = appNid
    if trackId not in testDisApps:
      notDiscoveredApps[app] = trackId
  
  print '###' * 10
  for appid in notDiscoveredApps.values():
    print 'not dis appid', appid
  print '###' * 10

  for pkgID, predictions in rst.iteritems():
    prediction = predictions[consts.APP_RULE]
    if prediction.label in notDiscoveredApps:
      notDiscoveredPkg.append((prediction.label, notDiscoveredApps[prediction.label],str(prediction.evidence)))
  notDiscoveredPkg = sorted(notDiscoveredPkg, key = lambda x: x[0])
  for diff in notDiscoveredPkg:
    print diff

def _output_rst(inforTrack):
  precision = inforTrack[consts.PRECISION]
  recall = inforTrack[consts.RECALL]
  appCoverage = inforTrack[consts.DISCOVERED_APP]
  f1Score = inforTrack[consts.F1SCORE]
  return 'Precision %s, Recall: %s, App: %s, F1 Score: %s' % (precision, recall, appCoverage, f1Score)

def auto_test():
  for testTbl in tbls:
    trainTbls = []
    for tbl in tbls:
      if tbl != testTbl:
        trainTbls.append(tbl)

    print trainTbls, testTbl
    inforTrack = cross_batch_test(trainTbls, testTbl, consts.IOS)
    output = _output_rst(inforTrack)
    log(trainTbls, testTbl, output)

def gen_rules():
  train(tbls, consts.IOS)
  log(tbls, 'NO TEST', 'PURE TRAIN')


if __name__ == '__main__':
  if len(sys.argv) < 2:
    print 'python test.py [auto|gen|test]'
  elif sys.argv[1] == 'auto':
    auto_test()
  elif sys.argv[1] == 'gen':
    gen_rules()
  elif sys.argv[1] == 'test':
    test(sys.argv[2])
