from utils import Relation
from utils import agent_clean
from sqldao import SqlDao
from package import Package
from utils import none2str
import re

def _get_app_features(pkgsegs):
	pkgftrs = {}

	for f, companies in pkgsegs.get().items():

		if len(companies) < 3:
			pkgftrs[f] = companies
	return pkgftrs

def _train(train_set):
	rst = {} # agent -> app
	relation = Relation()
	pkgsegs = Relation()
	seg2agent = Relation()
	agentftr = Relation()
	app2company = {}

	for package in train_set:
		app2company[package.app] = package.company
		if package.app in package.agent:
			rst[package.agent] = package.app
			agentftr.add(package.agent, package.app)
		agentsegs = filter(None, agent_clean(package.agent).split(' '))
		map(lambda x : pkgsegs.add(x, package.company), filter(None, package.app.split('.')))
		map(lambda x : pkgsegs.add(x, package.company), filter(None, none2str(package.company).split(' ')))
		map(lambda x : pkgsegs.add(x, package.company), filter(None, none2str(package.name).split(' ')))

		for seg in agentsegs:
			if '=' in seg: seg = seg.split('=')[1]

			seg2agent.add(seg, package.agent)
			relation.add(seg, package.app)

	# pkgftrs is a set of words that is only contained in one company
	pkgftrs = _get_app_features(pkgsegs)

	debug = []
	# k -> agent segment
	# v -> agents
	for k, v in relation.get().items():
		if len(v) == 1:
			app, count = v.popitem()
			if k in app or (k in pkgftrs and app2company[app] in pkgftrs[k]):
				map(lambda agent: rst.setdefault(agent[0], app), seg2agent.get()[k].items())
				for agent in seg2agent.get()[k].items():
					agentftr.add(agent[0], k)

			else:
				for f in app.split('.'):
					if f in pkgftrs and f in k:
						for agent in seg2agent.get()[k].items():
							rst[agent[0]] = app
							agentftr.add(agent[0], k)


	return _extract_general_frs(agentftr, rst)


	

def _extract_general_frs(agentftr, rst):
	tmprst = {}
	for agent, features in agentftr.get().items():
		agentsegs = filter(None, re.sub('[/\-v]?[0-9][0-9.]*', '/', agent).replace(';','/').replace('(','/').replace(')','/').split('/'))

		segfeatures = {seg.strip() for seg in agentsegs for f in features if f in seg and len(f) > 2}

		if rst[agent] in agent:
			tmprst[rst[agent]] = rst[agent]
		elif len(segfeatures):
			tmprst['$'.join(segfeatures)] = rst[agent]
	return tmprst

def mine_agent():
	QUERY = "select id, app, add_header, path, refer, hst, agent, company,name from packages where httptype=0"
	records = []
	sqldao = SqlDao()
	sqldao.execute('DELETE FROM rules WHERE agent IS NOT NULL')

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

	rst = _train(records)
	QUERY = 'INSERT INTO rules (app, agent) VALUES(%s, %s)'
	for k,v in rst.items():
		sqldao.execute(QUERY, (v,k))
	print 'Agents:', len(rst), 'Apps:', len(set(rst.values()))

class AgentRuler:
	def __init__(self):
		sqldao = SqlDao()
		QUERY = 'SELECT app, agent FROM rules WHERE agent IS NOT NULL'
		self.rules = {}
		for app, agent in sqldao.execute(QUERY):
			self.rules[agent] = app
	def classify(self, package):
		agentsegs = [seg.strip() for seg in filter(None, re.sub('[/\-v]?[0-9][0-9.]*', '/', package.agent).replace(';','/').replace('(','/').replace(')','/').split('/'))]
		for agentseg in agentsegs:
			if agentseg in self.rules:
				return self.rules[agentseg]
		return None

def test_agent():
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
	agent_classifier = AgentRuler()
	counter = 0
	correct = 0
	for record in records:
		rst = agent_classifier.classify(record)
		if rst:
			counter += 1
			if rst == record.app:
				correct += 1
			else:
				print rst,'#',record.app
			sqldao.execute('UPDATE packages SET classified = %s WHERE id = %s', (2, record.id))
	print counter, correct

if __name__ == '__main__':
	import sys
	def help():
		print 'agent.py train\tmining agent features and save them to db'
		print 'agent.py test\ttest agent features on db'
	if len(sys.argv) < 2:
		help()
	elif sys.argv[1] == 'train':
		mine_agent()
	elif sys.argv[1] == 'test':
		test_agent()
	else:
		help()
#mine_agent()
