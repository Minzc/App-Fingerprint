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

def _add_node(root, treePath, vnodeIndex, addInfo=None):
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
	if addInfo:
		for k,v in addInfo.items():
			appnode.set_addinfo(k,v)
			valuenode.set_addinfo(k,v)
	return appnode, valuenode

def _build_tree(package, vnodes, appnodes, root, kvs):
	
	hst = package.dst
	if package.secdomain != None:
		hst = package.secdomain

	app = package.app
	addInfo = {}
	if package.company:
		addInfo['company'] = package.company
	else:
		addInfo['company'] = package.app

	if hst != None:
		for key, values in kvs:
			for value in values:
				if len(value) > 0:
					treePath = (hst, key, app, value)
					appnode, valuenode = _add_node(root, treePath, vnodes, addInfo)
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
			cmpvalues.add(appnode.addInfo['company'])

		# keep one app or one company
		if len(appvalues) > 1 and len(cmpvalues) > 1:
			for appnode in vnode.get_father():
				appnode.get_father()[0].set_status(0)

	for appnode in appnodes:
		if len(appnode.get_all_child()) > 1:
			for tokennode in appnode.get_father():
				tokennode.set_status(-1)


def _gen_rules(root, serviceKey, confidence, support):
	hostnodes = root.get_all_child()
	rules = []
	for hostNode in hostnodes:
		hostName = hostNode.get_value()
		for tokenNode in hostNode.get_all_child():
			tokenName = tokenNode.get_value()
			validToken = False
			# number of app's use this token
			tokenSupport = len(tokenNode.get_all_child())
			tokenConfidence = 1.0 * tokenNode.counter / hostNode.counter
			print 'DEBUG', hostNode.value.encode('utf-8'), hostNode.counter, 'tokensupport:', tokenSupport, 'tokenConfidence:',tokenConfidence,tokenName.encode('utf-8')

			if tokenNode.get_status() == 1:
				# check token confidence and token support
				# if tokenConfidence < confidence or tokenSupport < support:
				if tokenSupport < support:
					continue
				validToken = True

			
			if hostName in serviceKey.get() and tokenName in serviceKey.get()[hostName] and tokenNode.get_status() != -1:
				validToken = True

			if validToken:
				for appNode in tokenNode.get_all_child():
					appName = appNode.get_value()
					appCompany = appNode.addInfo['company']
					for valueNode in appNode.get_all_child():
						# print 'value:', valuenode.get_value(), 'fathernum:',len(valuenode.get_father())
						valueName = valueNode.get_value()
						appCount = appNode.get_counter()
						rules.append((appName, appCompany, valueName, tokenName, hostName, tokenConfidence, tokenSupport))
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
	print 'DEBUG', serviceKey

	_prune_forest(vnodes, appnodes)

	rules = _gen_rules(root, serviceKey, confidence, support)

	# Persist
	sqldao = SqlDao()
	# app, value, token, host
	for appName, appCompany, valueName, tokenName, hostName, tokenConfidence, tokenSupport in rules:
		sqldao.execute('insert into features (host, company, token, app, value, confidence, support) values(%s,%s,%s,%s,%s,%s,%s)', (hostName, appCompany, tokenName, appName, valueName, tokenConfidence, tokenSupport))

	sqldao.close()

def _buildTestTree():
	sqldao = SqlDao()
	query = 'SELECT app, company, token, host, value FROM features'
	root = TreeNode(None, None)
	vnodes = {}

	# Build the tree
	for app, company, token, host, value in sqldao.execute(query):
		treePath = (host, token, value, app)
		_add_node(root, treePath, vnodes, {'company':company})	
	
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
						appNodes = treeNode.get_all_child() 
						if len(appNodes) == 1:
							predict = (appNodes[0],None)
							predict_app = (appNodes[0].get_value(), None)
						else:
							counter = FreqDist()
							for appNode in appNodes:
								counter.inc(appNode.addInfo['company'])
							predict = (appNodes[0], counter.max())
							predict_app = (None, counter.max())
							#TODO handle multiple rules
					
		if predict:
			# predict company
			if not predict[1]:
				if predict[1] != package.app:
					predict = predict[0]
					print predict.get_value(), app
					value = predict.get_father()[0].get_value()
					token = predict.get_father()[0].get_father()[0].get_value()
					host =  predict.get_father()[0].get_father()[0].get_father()[0].get_value()
					print "rules: %s\t%s\t%s" % (value, token, host)
			# predict app
			else:
				predict = predict[0]
				if predict.get_value() != package.company:
					print predict.get_value(), app
					value = predict.get_father()[0].get_value()
					token = predict.get_father()[0].get_father()[0].get_value()
					host =  predict.get_father()[0].get_father()[0].get_father()[0].get_value()
					print "rules: %s\t%s\t%s" % (value, token, host)
	# DEBUG			
	return predict_app

def test_algo(test_set = None):
	import urlparse

	root = _buildTestTree()
	correct = 0
	wrong = 0
	correct_ids = []
	
	if not test_set:
		test_set = load_pkgs()

	rst = {}
	for package in test_set:
		predictRst = _test(package, root)
		if not predictRst and package.refer:
			tmpHost = urlparse.urlparse(package.refer).netloc
			package.set_host(tmpHost)
			tmpQuery = urlparse.urlparse(package.refer).query
			tmpPath = urlparse.urlparse(package.refer).path
			package.set_path(tmpPath+'?'+tmpQuery)
			predictRst = _test(package, root)

		if predictRst:
			predict_app, predict_company = predictRst
			rst[package.id] = predictRst
			if predict_app == package.app or predict_company == package.company:
				correct += 1
				correct_ids.append(package.id)
			else:
				wrong += 1			

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