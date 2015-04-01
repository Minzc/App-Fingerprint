import algo
from package import Package
from sklearn.cross_validation import KFold
from sqldao import SqlDao
import path_algo
import classifier
from tools import tf_idf
import fp
from utils import load_pkgs

def merge_rst(rst, tmprst):
	for r in tmprst.keys():
		if r not in rst:
			rst[r] = tmprst[r]
	return rst

def evaluate(rst, test_set):
	
	# app_rst, record_id
	correct, wrong = 0, 0
	for k,v in rst.items():
		if v == test_set[k].app or test_set[k].company in set(v.split('$')) or v in test_set[k].name:
			correct += 1
		else:
			wrong += 1
	print 'Total:', len(test_set),'Recognized:', len(rst), 'Correct:', correct, 'Wrong:', wrong
	return correct

def use_classifier(classifier, test_set):
	rst = {}
	for id, record in test_set.items():
		# predict
		pApp = classifier.classify(record)
		if pApp:
			rst[id] = pApp
	return rst

def insert_rst(rst):
	QUERY = 'UPDATE packages SET classified = %s WHERE id = %s'
	sqldao = SqlDao()
	for k, v in rst.items():
		sqldao.execute(QUERY, (3, k))
	sqldao.close()

records = load_pkgs()

kf = KFold(len(records), n_folds=5, shuffle=True)

rnd=0

precision = 0
recall = 0

for train, test in kf:
	rnd += 1
	correct = 0
	print 'ROUND', rnd

	train_set = []
	test_set = {}
	rst = {}
	for i in train:
		train_set.append(records[i])
	for i in test:
		test_set[records[i].id] = records[i]
	
	#####################################
	#	FP Rules
	######################################
	fpClassifier = fp.mine_fp(train_set, 2, 0.8)
	rst = use_classifier(fpClassifier, test_set)
	#####################################
	# tf_idf(train_set)
	# path_algo.host_tree(train_set)
	# rst = classifier.classify(True, test_set.values())
	# correct = evaluate(rst, test_set)
	# algo.train(train_set)
	# tmprst = algo.test_algo(test_set.values())
	# rst = merge_rst(rst, tmprst)
	correct += evaluate(rst, test_set)
	precision += correct  * 1.0 /len(rst)
	recall += len(rst)  * 1.0 / len(test_set) * 1.0
	insert_rst(rst)

print 'Precision:', precision / 5.0, 'Recall:', recall / 5.0

