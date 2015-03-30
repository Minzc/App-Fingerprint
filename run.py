import algo
from package import Package
from sklearn.cross_validation import KFold
from sqldao import SqlDao
import path_algo
import classifier
from tools import tf_idf

def merge_rst(rst, tmprst):
	for r in tmprst.keys():
		if r not in rst:
			rst[r] = tmprst[r]
	return rst

def evaluate(rst, test_set):
	
	correct, wrong = 0, 0
	for k,v in rst.items():
		if v == test_set[k].app or test_set[k].company in set(v.split('$')) or v in test_set[k].name:
			correct += 1
		else:
			wrong += 1
	print 'Total:', len(test_set),'Recognized:', len(rst), 'Correct:', correct, 'Wrong:', wrong
	return correct

QUERY = "select id, app, add_header, path, refer, hst, agent, company,name from packages where httptype=0"
records = []
sqldao = SqlDao()

for id, app, add_header, path, refer, host, agent, company,name in sqldao.execute(QUERY):
	package = Package()
	package.set_app(app)
	package.set_path(path)
	package.set_id(id)
	package.set_add_header(add_header)
	package.set_refer(refer)
	package.set_host(host)
	package.set_agent(agent)
	package.set_company(company)
	package.set_name(name)
	records.append(package)

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
	
	tf_idf(train_set)
	path_algo.host_tree(train_set)
	rst = classifier.classify(True, test_set.values())
	correct = evaluate(rst, test_set)
	algo.train(train_set)
	tmprst = algo.test_algo(test_set.values())
	rst = merge_rst(rst, tmprst)
	correct += evaluate(rst, test_set)
	precision += correct  * 1.0 /len(rst)
	recall += len(rst)  * 1.0 / len(test_set) * 1.0


print 'Precision:', precision / 5.0, 'Recall:', recall / 5.0

sqldao.close()