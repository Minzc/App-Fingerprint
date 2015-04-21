from utils import load_pkgs
from utils import name_clean
from utils import Relation
from utils import none2str


def _mine_kw(records):
	rst = {} # agent -> app
	segs2app = Relation()
	segs2cmp = Relation()

	for package in records:
		map(lambda x : segs2app.add(x, package.app), filter(None, package.app.split('.')))
		map(lambda x : segs2cmp.add(x, package.company), filter(None, package.app.split('.')))
		# map(lambda x : segs2app.add(x, package.app), filter(None, name_clean(none2str(package.company)).split(' ')))
		# map(lambda x : segs2app.add(x, package.app), filter(None, name_clean(none2str(package.name)).split(' ')))
	appftrs = Relation()
	cmpftrs = Relation()

	#feature -> app
	for f, apps in segs2app.get().items():
		if len(apps) == 1:
			appftrs.add(apps.max(), f)
	for f, companies in segs2cmp.get().items():
		if len(companies) == 1:
			cmpftrs.add(companies.max(), f)
	# pkgftrs is a set of words that is only contained in one company
	return  appftrs, cmpftrs


def mine_txt(records):
	appfeatures, cmpfeatures = train(records)
	classifier = HostClassifier()
	classifier.addRules(appfeatures)
	classifier.addRules(cmpfeatures)
	return classifier

def train(records):
	appftrs, cmpftrs = _mine_kw(records)
	groupbyApp = Relation()
	groupbyCompany = Relation()
	for record in records:
		groupbyApp.add(record.host, record.app)
		groupbyCompany.add(record.host, record.company)

	appfeatures = {}
	for host, apps in groupbyApp.get().items():
		if len(apps) != 1:
			continue
		app = apps.max()
		for feature in appftrs.get().get(app, ()):
			if feature in host:
				appfeatures[host] = app
	cmpfeatures = {}
	for host, companies in groupbyCompany.get().items():
		if len(companies) != 1 or host in appfeatures:
			continue
		company = companies.max()

		for feature in cmpftrs.get().get(company, ()):
			if feature in host:
				cmpfeatures[host] = company

	return appfeatures, cmpfeatures
	# for k,v in appfeatures.items():
	# 	print k,v 
	# for k,v in cmpfeatures.items():
	# 	print k,v.encode('utf-8')
class HostClassifier:
	def __init__(self):
		self.ruleSet = []
	def addRules(self, rules):
		self.ruleSet.append(rules)
	def classify(self, record):
		lableDist = []
		for rules in self.ruleSet:
			if record.host in rules:
				lableDist.append(rules[record.host])
			else:
				lableDist.append(None)
		return lableDist


def testHostClassifier():
	records = load_pkgs()
	appfeatures, cmpfeatures = train(records)
	classifier = HostClassifier()
	classifier.addRules(appfeatures)
	classifier.addRules(cmpfeatures)
	appCounter = 0
	companyCounter = 0
	for record in records:
		lableDist = classifier.classify(record)
		if lableDist[0]:
			appCounter += 1
		elif lableDist[1]:
			companyCounter += 1
	print 'appCounter:', appCounter, 'companyCounter:', companyCounter, 'total:', len(records), 'sum:', appCounter+companyCounter


if __name__ == '__main__':
	testHostClassifier()