class TreeNode:
	def __init__(self, father, value):
		self.children = []
		self.father = [father]
		self.value = value
		self.status = 1
		self.counter=0
		self.leaf = []


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
		self.leaf.append(node)

	def get_all_leaf(self):
		return self.leaf

	def add_father(self, node):
		self.father.append(node)

def add_node(root, tree_path):
	tree_node = root
	host = tree_path[0]
	token = tree_path[1]

	while len(tree_path)>0:
		node_value = tree_path[0]
		tree_path = tree_path[1:]
		child_node = tree_node.get_child(node_value)
		# not adding leaf node
		if child_node == None:
			# not leaf node
			if len(tree_path) != 0:
				child_node = TreeNode(tree_node, node_value)
				tree_node.add_child(child_node)
			else:
				token_node = tree_node.get_father()[0]
				for app_node in token_node.get_all_leaf():
					if node_value == app_node.get_value():
						child_node = app_node
				if child_node == None:
					child_node = TreeNode(tree_node, node_value)
					tree_node.add_child(child_node)
					token_node.add_leaf(child_node)
				else:
					child_node.add_father(tree_node)
		

		child_node.inc_counter()
		tree_node = child_node
	# return valuenode
	return tree_node.get_father(), tree_node



def hostNToken():
	import urllib
	import mysql.connector
	from package import Package
	from nltk import FreqDist
	cnx=mysql.connector.connect(user='root',password='123',host='127.0.0.1',database='fortinet')
	cursor = cnx.cursor()
	query = "select app, add_header, hst, path, agent from packages where httptype = 0"
	cursor.execute(query)

	root = TreeNode(None, None)
	valuenode = set()
	appnode = set()

	# Build Tree
	for app, add_header, hst, path, agent in cursor:
		package = Package()
		package.set_path(path)
		package.set_host(hst)
		hst = package.secdomain

		if hst != None:
			for key, values in package.querys.items():			
				for value in values:
					if len(value) > 0:
						tree_path = (hst, key, value, app)
						value_node, app_node = add_node(root, tree_path)
						valuenode |= set(value_node)
						appnode.add(app_node)
		
		# tree_path = (hst, 'add_header', add_header, app)
		# app_node = add_node(root, tree_path)
		# valuenode.add(app_node)

		# tree_path = (hst, 'agent', agent, app)
		# app_node = add_node(root, tree_path)
		# valuenode.add(app_node)

	# Prune	
	for node in valuenode:
		if len(node.get_all_child()) > 1:
			for father in node.get_father():
				father.set_status(0)
	for node in appnode:
		if len(node.get_father()) > 1:
			for valuenode in node.get_father():
				valuenode.get_father()[0].set_status(0)

	# Persist
	hostnodes = root.get_all_child()
	
	for hostnode in hostnodes:
		host = hostnode.get_value()
		for tokennode in hostnode.get_all_child():
			token = tokennode.get_value()
			if tokennode.get_status() == 1:
				tkappnum = len(tokennode.get_all_child())
				for valuenode in tokennode.get_all_child():
					value = valuenode.get_value()
					for appnode in valuenode.get_all_child():
						app = appnode.get_value()
						count = appnode.get_counter()
						cursor.execute('insert into features (host, token, app, value, count, tkappnum) values(%s,%s,%s,%s, %s, %s)', (host, token, app, value, count, tkappnum))
	
	cnx.commit()
	cursor.close()
	cnx.close()

def test_algo():
	import urllib
	import mysql.connector
	from package import Package
	from nltk import FreqDist
	cnx=mysql.connector.connect(user='root',password='123',host='127.0.0.1',database='fortinet')
	cursor = cnx.cursor()
	query = 'select app, token, host, value from features_bak'
	cursor.execute(query)
	root = TreeNode(None, None)
	# Build the tree
	for app, token, host, value in cursor:
		tree_path = (host, token, value, app)
		add_node(root, tree_path)

	query = "select id, app, add_header, hst, path, agent from packages where httptype = 0"
	correct = 0
	wrong = 0
	total = 0
	cursor.execute(query)
	correct_ids = []
	for id, app, add_header, hst, path, agent in cursor:		
		package = Package()
		package.set_path(path)
		package.set_host(hst)
		hst = package.secdomain
		if hst != None:
			total += 1
			predict = None
			for key, values in package.querys.items():			
				for value in values:
					if len(value) > 0:
						tree_path = (hst, key, value)
						tree_node = root
						while len(tree_path) > 0 and tree_node != None:
							tree_node = tree_node.get_child(tree_path[0])
							tree_path = tree_path[1:]
						if tree_node != None:
							predict = tree_node.get_all_child()[0]
						

			if predict != None :
				if predict.get_value() == app:
					correct += 1
					correct_ids.append(id)
				else:
					print predict.get_value(), app
					value = predict.get_father()[0].get_value()
					token = predict.get_father()[0].get_father()[0].get_value()
					host =  predict.get_father()[0].get_father()[0].get_father()[0].get_value()
					print "rules: %s\t%s\t%s" % (value, token, host)
					wrong += 1

	upquery = "update packages set classified = %s where id = %s"	
	for id in correct_ids:
		cursor.execute(upquery, (1, id))
	cnx.commit()

	print 'Total:%s\tCorrect:%s\tWrong:%s' % (total, correct, wrong)



import sys						

if __name__ == '__main__':						
	if len(sys.argv) != 2:
		print 'Number of args is wrong'
	elif sys.argv[1] == 'test':
		test_algo()
	elif sys.argv[1] == 'train':
		hostNToken()
