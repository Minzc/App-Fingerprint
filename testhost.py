from host import HostApp
from host import Agent
from package import Package
from nltk import FreqDist
from sqldao import SqlDao
import sys
def test_train(writedb):
	hostcmp = HostApp()
	sqldao = SqlDao()
	

	query = 'select app, hst from packages group by app, hst '
	cursor = sqldao.execute(query)

	for app, host in cursor:
		package = Package()
		package.set_app(app)
		package.set_host(host)
		hostcmp.process(package)
	rst_hst_app, rst_hst_company = hostcmp.result()

	for k,v in rst_hst_app.items():
		print k,v

	if writedb:
		query = 'insert into test_rules (app, hst) values (%s, %s)'
		for k,v in rst_hst_app.items():
			sqldao.execute(query, (v,k))

		query = 'insert into test_rules (app, company, hst) values (%s,%s, %s)'
		for host,apps in rst_hst_company.items():
			for app, company in apps:
				sqldao.execute(query, (app,company, host))

	sqldao.close()

def test_classify():
	sqldao = SqlDao()
	query = 'select app, company, hst from test_rules'
	cursor = sqldao.execute(query)
	host_rules = {}
	company_rules = {}
	
	for app, company, host in cursor:
		if app != '':
			host_rules[host] = app
		if company != '':
			company_rules[host] = company
		

	query = 'select app, hst from packages'
	cursor = sqldao.execute(query)
	app_correct = 0
	company_correct = 0
	total = 0
	for app, host in cursor:
		if host in host_rules or host in company_rules:
			total += 1
			if host_rules.get(host,'') == app:
				app_correct += 1
			elif company_rules.get(host,'') in app:
				company_correct += 1
			else:
				print 'Wrong:',app,host_rules[host]
	print 'Total:%s\tAppCorrect:%s\tHostCorrect:%s' % (total, app_correct, company_correct)

def test_cluster():
	from cluster import ClusterPkgName
	from cluster import ClusterHost
	from host import HostAnalyz

	host_analyz = HostAnalyz()
	pkg_cluster = ClusterPkgName()
	hst_cluster = ClusterHost()
	sqldao = SqlDao()
	query = 'select app, hst from packages'
	cursor = sqldao.execute(query)
	for app, hst in cursor:
		package = Package()
		package.set_host(hst)
		package.set_app(app)
		hst_cluster.process(package)
		pkg_cluster.process(package)

	pkg_rst = pkg_cluster.result()
	hst_rst = hst_cluster.result(pkg_rst)
	rst = host_analyz.analyz_clst(hst_rst)
	for apps,hostlsts in rst.items():
		print apps, "$$$$$", ','.join(hostlsts)
		print

def test_agent(writedb):
	agnt = Agent()
	sqldao = SqlDao()
	

	query = 'select app, hst, agent from packages where app = \'com.acmeaom.android.myradar\' group by app, agent'
	cursor = sqldao.execute(query)

	for app, hst, agent in cursor:
		package = Package()
		package.set_app(app)
		package.set_agent(agent)
		package.set_host(hst)
		agnt.process(package)
	rst = agnt.result()

	if writedb:
		query = 'insert into test_rules (app, agent) values (%s, %s)'
		for app,agent in rst:
			sqldao.execute(query, (app,agent))

if __name__ == '__main__':

	if sys.argv[1] == 'train':
		test_train(False)
	elif sys.argv[1] == 'classify':
		test_classify()
	elif sys.argv[1] == 'cluster':
		test_cluster()
	elif sys.argv[1] == 'agent':
		test_agent(False)