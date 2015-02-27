from package import Package
from nltk import FreqDist
from cluster import ClusterHost
import re
from utils import app_clean


class Agent:
	def __init__(self):
		self.content = []
		
	def agent_clean(self, agent):
		# remove incomplete brackets
		agent = re.sub('\\([^\\)]*?$','',agent)
		agent = re.sub('\\(',' ',agent)
		agent = re.sub('\\)',' ',agent)
		agent = re.sub('/[^ ]*','',agent)
		agent = re.sub('[0-9][0-9.]*','',agent)
		agent = agent.replace(';',' ')
		agent = agent.replace('.',' ')
		agent = agent.replace('-', ' ')
		agent = agent.replace('_', ' ')
		return agent

	def process(self, package):
		self.content.append((package.app, package.agent, package.name))

	def result(self):
		import re
		from nltk import FreqDist
		counter = FreqDist()
		orig_agent = {} # agent_id -> agent
		agent_app = {}  # agent -> app
		occured = set()

		for apppkg, agent,name in self.content:
			app = app_clean(apppkg)
			oriagent = agent

			if oriagent not in agent_app:
				agent_app[oriagent] = apppkg

				agent = self.agent_clean(agent)

				idstr = ''
				appsegs = app.split('.')
				namesegs = name.replace('android','').replace('free','').replace('-', ' ').replace(':', ' ').split(' ')

				
				for seg in agent.split(' '):
	  				seg=seg.split('-')[0].split(':')[0]
	  				
		  			if len(seg) > 1 and (seg in appsegs or seg in namesegs):
						idstr = idstr+ ' ' + seg
		  			else:
		  				pass
						# print '$$$$$',seg.encode('utf-8')

				print idstr
				if len(idstr) > 0:
					if app + '$' + idstr not in occured:
		  				counter.inc(idstr)
		  				occured.add(app + '$' + idstr)

		  			orig_agent.setdefault(idstr, set())
		  			orig_agent[idstr].add(oriagent)

		  		if name in oriagent:
		  			orig_agent.setdefault(oriagent, set())
		  			orig_agent[oriagent].add(oriagent)


		rst = []
		for k,v in counter.items():
	  		if v == 1:
	  			for x in orig_agent[k]:
	  				rst.append((agent_app[x], x.encode('utf-8'),k))
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
		name = package.name

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
			for hstseg in hstsegs:
				if appseg == hstseg:
					self.app_hosts[app].add(host)
				if appseg in hstseg or hstseg in appseg and len(appseg) > 2 and len(hstseg) > 2:
					self.app_hosts[app].add(host)
		
		name = name.replace(' ','')
		if name in host:
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

	# def clustr_analyz(self):
	# 	rst = self.clusterhost.result()
	# 	hst_clstid = {}
	# 	clsters = []
		
	# 	clstid = 0
	# 	for apps, hosts in rst.items():
	# 		for host in hosts.split(','):
	# 			hst_clstid.setdefault(host, set())
	# 			hst_clstid[host].add(clstid)
	# 		clsters.append(apps)
	# 		clstid += 1

	# 	# find hosts that occured in only one cluster
	# 	clstid_host = {}
	# 	for host, clstids in hst_clstid.items():
	# 		if len(clstids) == 1:
	# 			clstid = clstids.pop()
	# 			clstid_host.setdefault(clstid, set())
	# 			clstid_host[clstid].add(host)
		
	# 	host_app = {}
	# 	host_company = {}
	# 	for clstid,hosts in clstid_host.items():
	# 		if len(hosts) == 1:
	# 			print "$$$", hosts
	# 			host = hosts.pop()
	# 			if len(clsters[clstid]) == 1:
	# 				host_app[host] = clsters[clstid]
	# 			else:
	# 				clstname = self.get_cluster_name(clsters[clstid])	
	# 				host_company[host] = set()
	# 				for app in clsters[clstid]:
	# 					host_company[host].add((app,clstname))
	# 	return host_app, host_company
				

	
	def get_cluster_name(self, apps):
		"""
		input : a set of app names
		"""
		names = set()
		for app in apps:
			cmpname = app_clean(app).split('.')[0]
			names.add(cmpname)
		if len(names) == 1:
			return names.pop()
		else:
			# TODO get company name from url
			return apps.pop()

	# def app_clean(self, appname):
	# 	return appname.replace('air.','')\
	# 	.replace('com.','')\
	# 	.replace('br.','')\
	# 	.replace('net.','')\
	# 	.replace('au.','')\
	# 	.replace('ca.','')\
	# 	.replace('cn.','')\
	# 	.replace('co.','')\
	# 	.replace('org','')

class HostAnalyz:
	def analyz_clst(self, clusters):
		"""
		remove general hosts
		Input: app1, app2 \t host1, host2
		"""
		from nltk import FreqDist
		import tldextract
		counter = FreqDist()
		for apps, hosts in clusters.items():
			secdomains = set()
			for host in hosts.split(','):
				# counter.inc(host)
				extracted = tldextract.extract(host)
				secdomain = None

				if len(extracted.domain) > 0:
					secdomain = "{}.{}".format(extracted.domain, extracted.suffix)
					secdomains.add(secdomain)
			for secdomain in secdomains:
				counter.inc(secdomain)

		rst = {}
		for apps, hosts in clusters.items():
			hosts = hosts.split(',')
			rst[apps] = set()
			for host in hosts:
				extracted = tldextract.extract(host)
				secdomain = None
				if len(extracted.domain) > 0:
					secdomain = "{}.{}".format(extracted.domain, extracted.suffix)					
					if counter[secdomain] == 1:
						rst[apps].add(host)
				# if counter[host] == 1:
				# 	rst[apps].add(host)
		return rst
