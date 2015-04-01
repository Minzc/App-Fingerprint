from utils import load_tfidf
from sqldao import SqlDao
from utils import processPath
import math
from utils import Relation
from nltk import FreqDist
import urllib
import mysql.connector
from package import Package
from utils import app_clean
from utils import load_pkgs
class TreeNode:
	def __init__(self, father, value):
		self.children = []
		self.father = [father]
		self.value = value
		self.status = 1
		self.counter=0
		self.leaf = {}
		self.addInfo = {}
	def set_addinfo(self, key, value):
		self.addInfo[key] = value

	def inc_counter(self):
		self.counter += 1
	def get_counter(self):
		return self.counter

	def get_value(self):
		return self.value

	def add_child(self, treenode):
		self.children.append(treenode)

	def get_child(self, value):
		for child in self.children:
			if child.value == value:
				return child
		return None

	def get_all_child(self):
		return self.children

	def get_father(self):
		return self.father

	def set_status(self, status):
		self.status = status

	def get_status(self):
		return self.status
	def to_string(self):
		return ','.join([child.get_value() for child in self.children])

	def add_leaf(self, node):
		self.leaf[node.get_value()] = node

	def get_all_leaf(self):
		return self.leaf

	def add_father(self, node):
		self.father.append(node)

def _add_node(root, treePath, vnodeIndex, addinfo=None):
	tree_node = root
	host = treePath[0]
	token = treePath[1]
	appnode = None
	valuenode = None

	for i in range(len(treePath)):
		node_value = treePath[i]
		child_node = tree_node.get_child(node_value)
		
		# not adding leaf node
		if child_node == None:
			# not leaf node
			if i == 0 or i == 1 or i == 2:
				child_node = TreeNode(tree_node, node_value)
				tree_node.add_child(child_node)
			elif i == 3: # value node shared among the forest
				child_node = vnodeIndex.get(node_value, None)
				if child_node == None:
					child_node = TreeNode(tree_node, node_value)
					vnodeIndex[node_value] = child_node
				else:
					child_node.add_father(tree_node)
				tree_node.add_child(child_node)

		child_node.inc_counter()
		tree_node = child_node
		if i == 3:
			valuenode = tree_node
		elif i == 2:
			appnode = tree_node
	if addinfo:
		for k,v in addinfo.items():
			appnode.set_addinfo(k,v)
	return appnode, valuenode

def _build_tree(package, vnodes, appnodes, root, kvs):
	
	hst = package.dst + '$' + package.path
	if package.secdomain != None:
		hst = package.secdomain + '$' + package.path

	app = package.app
	addinfo = {}
	addinfo['company'] = package.company

	if hst != None:
		for key, values in kvs:
			for value in values:
				if len(value) > 0:
					treePath = (hst, key, app, value)
					appnode, valuenode = _add_node(root, treePath, vnodes, addinfo)
					# vnodes[valuenode.get_value()] = valuenode
					appnodes.append(appnode)
	return appnodes, vnodes

def _prune_forest(vnodes, appnodes):
	# Prune	
	for vnode in vnodes.values():
		appvalues = set()
		cmpvalues = set()
		for appnode in vnode.get_father():
			appvalues.add(appnode.get_value())
			cmpvalues.add(appnode.addInfo.get('company',''))

		if (len(appvalues) > 1 and len(cmpvalues) > 1) or len(vnode.get_value()) < 3:
			for appnode in vnode.get_father():
				appnode.get_father()[0].set_status(0)

	for appnode in appnodes:
		if len(appnode.get_all_child()) > 1:
			for tokennode in appnode.get_father():
				tokennode.set_status(0)


def _gen_rules(root, serviceKey, confidence, support):
	hostnodes = root.get_all_child()
	rules = []
	for hostNode in hostnodes:
		hostName, path = hostNode.get_value().split('$')
		for tokenNode in hostNode.get_all_child():
			tokenName = tokenNode.get_value()
			validToken = False
			tokenConfidence = 1.0 * tokenNode.counter / hostNode.counter
			if tokenNode.get_status() == 1:
				# check token confidence and token support
				if tokenConfidence < confidence or tokenNode.counter < support:
					continue
				validToken = True

			tokenSupport = tokenNode.get_value()
			if hostName in serviceKey.get() and tokenSupport in serviceKey.get()[hostName]:
				print '###ok'
				validToken = True

			if validToken:
				for appNode in tokenNode.get_all_child():
					appName = appNode.get_value()
					for valueNode in appNode.get_all_child():
						# print 'value:', valuenode.get_value(), 'fathernum:',len(valuenode.get_father())
						valueName = valueNode.get_value()
						appCount = appNode.get_counter()
						rules.append((appName, valueName, tokenName, hostName, tokenConfidence, tokenSupport))
	return rules

def hostNToken(training_data=None, confidence=0.8, support=2):

	sqldao = SqlDao()
	sqldao.execute('TRUNCATE TABLE features')

	if not training_data:
		training_data = load_pkgs()
	
	vnodes = {}
	appnodes = []
	root = TreeNode(None, None)
	serviceKey = Relation()

	# Build the forest
	for package in training_data:
		appnode, vnodes = _build_tree(package, vnodes, appnodes, root, package.querys.items())
		for k,v in package.querys.items():
			if package.app in v or package.name in v:
				serviceKey.add(package.secdomain, k)
				
	_prune_forest(vnodes, appnodes)

	rules = _gen_rules(root, serviceKey, confidence, support)

	# Persist
	sqldao = SqlDao()
	# app, value, token, host
	for appName, valueName, tokenName, hostName, tokenConfidence, tokenSupport in rules:
		sqldao.execute('insert into features (host, token, app, value, confidence, support) values(%s,%s,%s,%s,%s,%s)', (hostName, tokenName, appName, valueName, tokenConfidence, tokenSupport))

	sqldao.close()


def train(training_data=None):

	root = TreeNode(None, None)
	vnodes = {}
	appnodes = []

	sqldao = SqlDao()
	sqldao.execute('TRUNCATE TABLE features')
	QUERY = 'SELECT app, add_header, hst FROM packages WHERE httptype = 0 and add_header != \'\''

	if not training_data:
		sqldao = SqlDao()
		# Build Tree
		for app, add_header,hst in sqldao.execute(QUERY):
			package = Package()
			package.set_app(app)
			package.set_host(hst)
			
			kvs = {}
			splitpos = add_header.find(':')
			key = add_header[0:splitpos].strip()
			value = add_header[splitpos+1:].strip()
			kvs[key] = [value]
			
			appnode, vnodes = _build_tree(package, vnodes, appnodes, root, kvs.items())
	else:
		for package in training_data:
			appnode, vnodes = _build_tree(package, vnodes, appnodes, root)

	# Prune	
	for vnode in vnodes.values():
		appvalues = set()
		for appnode in vnode.get_father():
			appvalues.add(appnode.get_value())
		if len(appvalues) > 1 or len(vnode.get_value()) < 3:
			for appnode in vnode.get_father():
				# print 'ok', appnode.get_father()[0].get_value()
				appnode.get_father()[0].set_status(0)
				if appnode.get_father()[0].get_value() == 'X-Requested-With':
					print '$$$', vnode.get_value(), appvalues,appnode.get_father()[0].get_father()[0].get_value()

	for appnode in appnodes:
		if len(appnode.get_all_child()) > 1:
			for tokennode in appnode.get_father():
				tokennode.set_status(0)

	# Persist
	hostnodes = root.get_all_child()
	
	sqldao = SqlDao()
	for hostnode in hostnodes:
		host = hostnode.get_value()
		for tokennode in hostnode.get_all_child():
			token = tokennode.get_value()
			if tokennode.get_status() == 1:
				tkappnum = len(tokennode.get_all_child())
				# print 'tkappnum', tkappnum
				for appnode in tokennode.get_all_child():
					app = appnode.get_value()
					# print 'app:',app,'childnum', len(appnode.get_all_child())
					for valuenode in appnode.get_all_child():
						# print 'value:', valuenode.get_value(), 'fathernum:',len(valuenode.get_father())
						value = valuenode.get_value()
						count = appnode.get_counter()
						sqldao.execute('insert into features (host, header, app, value, count, tkappnum) values(%s,%s,%s,%s, %s, %s)', (host, token, app, value, count, tkappnum))
	sqldao.close()

def _buildTestTree():
	sqldao = SqlDao()
	query = 'SELECT app, token, host, value FROM features'
	root = TreeNode(None, None)
	vnodes = {}
	serviceKey = {}

	# Build the tree
	for app, token, host, value in sqldao.execute(query):
		treePath = (host, token, value, app)
		add_node(root, treePath, vnodes)
	sqldao.close()
	return root

def _test(package, root):
	hst = package.secdomain
	app = package.app
	company = package.company
	predict_app = None
	
	if hst:
		predict = None
		for key, values in package.querys.items():			
			for value in values:
				if value:
					treePath = (hst, key, value)
					treeNode = root
					while len(treePath) > 0 and treeNode != None:
						treeNode = treeNode.get_child(treePath[0])
						treePath = treePath[1:]
					# treeNode is valuenode
					if treeNode:
						predict = treeNode.get_all_child()[0]
					

		if predict:
			predict_app = predict.get_value()
				
	return predict_app

def _get_test_set():
	query = "SELECT id, app, add_header, hst, path, agent, name, company FROM packages WHERE httptype = 0"
	sqldao = SqlDao()
	test_set = []
	for id, app, add_header, hst, path, agent,name,company in sqldao.execute(query):		
		package = Package()
		package.set_id(id)
		package.set_app(app)
		package.set_path(path)
		package.set_host(hst)
		package.set_name(name)
		package.set_company(company)
		test_set.append(package)
	sqldao.close()
	return test_set

def test_algo(test_set = None):

	root = _buildTestTree()
	correct = 0
	wrong = 0
	correct_ids = []
	
	if not test_set:
		test_set = _get_test_set()

	rst = {}
	for package in test_set:
		predict_app = _test(package, root)
		if predict_app:
			rst[package.id] = predict_app
			if predict_app == package.app or predict_app == package.company:
				correct += 1
				correct_ids.append(package.id)
			else:
				wrong += 1
				# print predict.get_value(), app
				# value = predict.get_value()
				# token = predict.get_father()[0].get_value()
				# host =  predict.get_father()[0].get_father()[0].get_value()
				# print "rules: %s\t%s\t%s" % (value, token, host)

	upquery = "update packages set classified = %s where id = %s"	
	sqldao = SqlDao()
	for pid in correct_ids:
		sqldao.execute(upquery, (10, pid))
	sqldao.close()

	print 'Total:%s\tCorrect:%s\tWrong:%s' % (len(test_set), correct, wrong)
	return rst





import sys						

if __name__ == '__main__':						
	if len(sys.argv) != 2:
		print 'Number of args is wrong'
	elif sys.argv[1] == 'test':
		test_algo()
	elif sys.argv[1] == 'train':
		hostNToken()
	elif sys.argv[1] == 'header':
		hostNheader()


# def decisiontree(records, tfidf):
# 	"""
# 	[label, set(feature1, feature2, ..., featureN)]
# 	"""
# 	rules = {}
# 	gloablfeatures = FreqDist()
# 	for record in records:
# 		for f in record[1]:
# 			if len(f) > 0:
# 				gloablfeatures.inc(f)
# 	while(len(records) > 0):
# 		features = FreqDist()
# 		for record in records:
# 			for f in record[1]:
# 				if len(f) > 0:
# 					features.inc(f)
# 		mine = entrophy(records)
# 		if mine == 0:
# 			"""
# 			If there is only one class in records, select the most frequent feature
# 			in the set. If many features have the same frequency, select the one that
# 			is less frequent in the global set.
# 			"""
# 			score = 0
# 			bestf = None
# 			app,_ = records[0][0].split('$')
# 			for f, v in features.items():
# 				if tfidf[f][app] > score:
# 					bestf = f
# 					score = tfidf[f][app]
# 			rules[bestf] = records[0][0]
# 			print '1. bestf is ', bestf
# 			break
# 		else:
# 			bestf = None
# 			total = len(records) * 1.0
# 			class_one = []
# 			class_two = []
# 			for feature in features.keys():
# 				if feature in rules:
# 					continue
# 				have, other = [], []
# 				for record in records:
# 					if feature in record[1]:
# 						have.append(record)
# 					else:
# 						other.append(record)

# 				e = len(have) / total * entrophy(have) + len(other) / total * entrophy(other)
# 				print feature, e, mine, e ==mine
# 				if e < mine:
# 					mine = e
# 					bestf = feature
# 				elif e == mine and (features[bestf] > features[feature] or bestf == None):
# 					bestf = feature

# 			print '2. bestf is ', bestf
# 			if bestf:
# 				accuracy = FreqDist()
# 				for record in records:
# 					if bestf in record[1]:
# 						accuracy.inc(record[0])
# 						class_one.append(record)
# 					else:	
# 						class_two.append(record)
# 				if accuracy.values()[0] * 1.0 / len(class_one) >= 0.9:
# 					rules[bestf] = accuracy.keys()[0]
# 					records = class_two
# 				else:
# 					break
# 			else:
# 				break
# 	return rules



# def entrophy(records):
	
# 	total = len(records) * 1.0
# 	counter = FreqDist()
# 	for record in records:
# 		counter.inc(record[0])
# 	e = 0
# 	for k,v in counter.items():
# 		e += (v/total) * math.log((v/total))
# 	return e * -1.0