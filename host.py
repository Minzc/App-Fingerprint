from package import Package
from nltk import FreqDist
from cluster import ClusterHost
from utils import app_clean
import re


class Agent:
	def __init__(self):
		self.content = []
		
	def process(self, package):
		self.content.append((package.app, package.agent))

	def result(self):
		import re
		from nltk import FreqDist
		counter = FreqDist()
		uccur = set()
		orig_agent = {} # agent_id -> agent
		agent_app = {}  # agent -> app


		for app, agent in self.content:
			app = app.lower()
			oriagent = agent.lower()
			agent_app[oriagent] = app
			agent = agent.lower()

			# remove incomplete brackets
			agent = re.sub('\\([^\\)]*?$','',agent)
			agent = re.sub('\\(','',agent)
			agent = re.sub('\\)','',agent)
			agent = re.sub('/[^ ]*','',agent)
			agent = agent.replace('.', ' ')
			idstr = ''
			
			for seg in agent.split(' '):
  				seg=seg.split('-')[0].split(':')[0]
	  			if len(seg) > 1 and seg in app and app+'$'+ agent not in uccur:
					idstr = idstr+ ' ' + seg
	  			else:
	  				pass
					# print '$$$$$',seg.encode('utf-8')
			if len(idstr) > 0:
	  			counter.inc(idstr)
	  			orig_agent[idstr] = oriagent
		rst = []
		for k,v in counter.items():
	  		if v == 1:
	  			rst.append((agent_app[orig_agent[k]], orig_agent[k].encode('utf-8')))
				print agent_app[orig_agent[k]], orig_agent[k].encode('utf-8')
		print rst
		return rst
		

class HostApp:
	def __init__(self):
		self.host_apps = {}
		self.host_company = {}
		self.app_hosts = {}
		self.clusterhost = ClusterHost()

	def process(self, package):
		topdom = package.secdomain
		app = package.app
		host = package.host
		self.clusterhost.process(package)

		self.host_apps.setdefault(host,set())
		self.host_company.setdefault(host,set())
		self.app_hosts.setdefault(app,set())

		appsegs = self.app_clean(app.lower()).split('.')		
		self.host_apps[host].add(app)
		self.host_company[host].add(appsegs[0])

		hstsegs = set(package.host.lower().replace('.',' ').replace('-',' ').split(' '))

		# they contain common part
		for appseg in appsegs:
			if appseg in hstsegs:
				self.app_hosts[app].add(host)

	def result(self):
		"""
		rst_hst_app: host -> app
		rst_hst_company: host -> {app:company, app:company}
		"""
		rst_hst_app = {}
		rst_hst_company = {}
		for app, hosts in self.app_hosts.items():
			for host in hosts:
				if len(self.host_apps[host]) == 1:
					rst_hst_app[host] = app
				elif len(self.host_company[host]) < 2:

					# print self.host_apps[host]
					# print self.host_company[host]
					rst_hst_company.setdefault(host, set())
					company = self.host_company[host].pop()
					rst_hst_company[host].add((app, company))
					self.host_company[host].add(company)
				else:
					print self.host_apps[host], host, app
		
		# host_app, host_company = self.clustr_analyz()
		# print "$$$$$", host_app
		# print '$$$$$', host_company
		# for host, apps in host_app.items():
		# 	rst_hst_app[host] = apps.pop()

		# for host ,apps in host_company.items():		
		# 	rst_hst_company.setdefault(host, set())
		# 	for app, company in apps:
		# 		rst_hst_company[host].add((app, company))


		return rst_hst_app, rst_hst_company

	def clustr_analyz(self):
		rst = self.clusterhost.result()
		hst_clstid = {}
		clsters = []
		
		clstid = 0
		for hosts, apps in rst.items():
			for host in hosts.split(','):
				hst_clstid.setdefault(host, set())
				hst_clstid[host].add(clstid)
			clsters.append(apps)
			clstid += 1

		# find hosts that occured in only one cluster
		clstid_host = {}
		for host,clstids in hst_clstid.items():
			if len(clstids) == 1:
				clstid = clstids.pop()
				clstid_host.setdefault(clstid, set())
				clstid_host[clstid].add(host)
		
		host_app = {}
		host_company = {}
		for clstid,hosts in clstid_host.items():
			if len(hosts) == 1:
				print "$$$", hosts
				host = hosts.pop()
				if len(clsters[clstid]) == 1:
					host_app[host] = clsters[clstid]
				else:
					clstname = self.get_cluster_name(clsters[clstid])	
					host_company[host] = set()
					for app in clsters[clstid]:
						host_company[host].add((app,clstname))
		return host_app, host_company
				

	
	def get_cluster_name(self, apps):
		"""
		input : a set of app names
		"""
		names = set()
		for app in apps:
			cmpname = self.app_clean(app).split('.')[0]
			names.add(cmpname)
		if len(names) == 1:
			return names.pop()
		else:
			# TODO get company name from url
			return apps.pop()

	def app_clean(self, appname):
		return appname.replace('air.','')\
		.replace('com.','')\
		.replace('br.','')\
		.replace('net.','')\
		.replace('au.','')\
		.replace('ca.','')\
		.replace('cn.','')\
		.replace('co.','')\
		.replace('org','')

class HostAnalyz:
	def analyz_clst(self, clusters):
		"""
		remove general hosts
		Input: app1, app2 \t host1, host2
		"""
		from nltk import FreqDist
		counter = FreqDist()
		for apps, hosts in clusters.items():
			for host in hosts.split(','):
				counter.inc(host)
		rst = {}
		for apps, hosts in clusters.items():
			hosts = hosts.split(',')
			rst[apps] = set()
			for host in hosts:
				if counter[host] == 1:
					rst[apps].add(host)
		return rst
