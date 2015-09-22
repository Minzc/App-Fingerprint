from run_modify import cross_batch_test
from run_modify import TRAIN_LABEL
from run_modify import USED_CLASSIFIERS
from run_modify import train
import const.consts as consts
import sys

tbls = [  'ios_packages_2015_08_10', 'ios_packages_2015_06_08', 'ios_packages_2015_08_12', 'ios_packages_2015_08_04']
# tbls = ['ios_packages_2015_08_12', 'ios_packages_2015_08_10']


def log(trainTbls, testTbl, output):
  fw = open('autotest_ios.txt', 'a')
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
  output = cross_batch_test(trainTbls, testTbl, consts.IOS)
  log(trainTbls, testTbl, output)

def auto_test():
  for testTbl in tbls:
    trainTbls = []
    for tbl in tbls:
      if tbl != testTbl:
        trainTbls.append(tbl)

    print trainTbls, testTbl
    output = cross_batch_test(trainTbls, testTbl, consts.IOS)
    log(trainTbls, testTbl, output)

def gen_rules():
  train(tbls, consts.IOS)
  log(tbls, 'NO TEST', 'PURE TRAIN')


if __name__ == '__main__':
  if len(sys.argv) != 2:
    print 'python test.py [auto|gen|test]'
  elif sys.argv[1] == 'auto':
    auto_test()
  elif sys.argv[1] == 'gen':
    gen_rules()
  elif sys.argv[1] == 'test':
