from run_modify import cross_batch_test
from run_modify import TRAIN_LABEL
from run_modify import USED_CLASSIFIERS
import const.consts as consts

tbls = [  'ios_packages_2015_08_10', 'ios_packages_2015_06_08', 'ios_packages_2015_08_12', 'ios_packages_2015_08_04']
# tbls = ['ios_packages_2015_08_12', 'ios_packages_2015_08_10']


for test_tbl in tbls:
  fw = open('autotest_ios.txt', 'a')
  train_tbls = []
  for tbl in tbls:
    if tbl != test_tbl:
      train_tbls.append(tbl)

  #if test_tbl != 'packages_20150429':
  # continue
  print train_tbls, test_tbl
  output = cross_batch_test(train_tbls, test_tbl, consts.IOS)
  fw.write(str(train_tbls)+' ' + test_tbl+'\n')
  fw.write(str(trainedLabel) + '\n')
  fw.write(str(trainedClassifiers) + '\n')
  fw.write(output + '\n')
  fw.write('=' * 20 + '\n')
  fw.close()
  break
