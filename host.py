from collections import defaultdict
import tldextract
from cluster import ClusterHost
from utils import app_clean, load_pkgs


class HostApp:
    def __init__(self):
        self.host_apps = {}
        self.host_company = {}
        self.app_hosts = {}
        self.clusterhost = ClusterHost()
    
    def train(self, records):
      for record in records:
        self._process(record)
      self.host_apps, self.host_company = self._result()

    def classify(self, record):
      label_dist = {}
      if record.host in self.host_apps:
        return {self.host_apps[record.host]:1.0}
      return {}

    def _process(self, package):
        topdom = package.secdomain
        app = package.app
        host = package.host
        name = package.name

        self.clusterhost.process(package)

        self.host_apps.setdefault(host, set())
        self.host_company.setdefault(host, set())
        self.app_hosts.setdefault(app, set())

        appsegs = app_clean(app.lower()).split('.')
        self.host_apps[host].add(app)
        self.host_company[host].add(appsegs[-1])

        hstsegs = set(package.host.lower().replace('.', ' ').replace('-', ' ').split(' '))

        # they contain common part
        for appseg in appsegs:
            for hstseg in hstsegs:
                appseg = appseg.strip()
                hstseg = hstseg.strip()
                if appseg == hstseg and len(appseg) > 0:
                    self.app_hosts[app].add(host)
                if (appseg in hstseg or hstseg in appseg) and len(appseg) > 2 and len(hstseg) > 2:
                    self.app_hosts[app].add(host)

        for nameseg in name.split(','):
            nameseg = nameseg.replace(' ', '')
            if nameseg in host:
                self.app_hosts[app].add(host)

    def _result(self):
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
                    rst_hst_company.setdefault(host, set())
                    company = self.host_company[host].pop()
                    rst_hst_company[host].add((app, company))
                    self.host_company[host].add(company)
        return rst_hst_app, rst_hst_company

    # def clustr_analyz(self):
    # rst = self.clusterhost.result()
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
        counter = defaultdict(int)
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
                    # rst[apps].add(host)
        return rst

if __name__ == '__main__':
  records = load_pkgs()
  miner = HostApp()
  for record in records:
    miner.process(record)
  r1, r2 = miner.result()
  for k,v in r1.iteritems():
    print k, v
