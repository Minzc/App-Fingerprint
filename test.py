from run import cross_batch_test

tbls = ['ios_packages_2015_08_12', 'ios_packages_2015_08_10', 'ios_packages_2015_08_04', 'ios_packages_2015_06_08']


for test_tbl in tbls:
	fw = open('autotest_ios.txt', 'a')
	train_tbls = []
	for tbl in tbls:
		if tbl != test_tbl:
			train_tbls.append(tbl)

	#if test_tbl != 'packages_20150429':
	#	continue
	print train_tbls, test_tbl
	output = cross_batch_test(train_tbls, test_tbl)
	fw.write(str(train_tbls)+' ' + test_tbl+'\n')
	fw.write(output + '\n')
	fw.write('=' * 20 + '\n')
	fw.close()
