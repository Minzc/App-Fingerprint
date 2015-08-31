from classifier import AbsClassifer
from app_info import AppInfos
import consts
from collections import defaultdict
class TextClassifier(AbsClassifer):
  def train(self):
    def count(segs_str):
      for i in range(len(pkgSegs)):
        feature = ''
        for j in range(i, len(pkgSegs)):
          feature += pkgSegs[j]
          fAppCounter[feature].add(app.package)
          fCompanyCounter[feature].add(app.company)

    appInfos = AppInfos
    fAppCounter = defaultdict(set)
    fCompanyCounter = defaultdict(set)
    for key, app in appInfos[consts.IOS].items():
      pkgSegs = app.package.split('.')
      count(pkgSegs)
      nameSegs = app.name.split(' ')
      count(nameSegs)
      companySegs = app.company.split(' ')
    
    rules = {consts.APP_RULE:{}, consts.COMPANY_RULE:{}, consts.CATEGORY_RULE:{}}
    for seg, apps in fAppCounter.iteritems():
      if len(apps) == 1:
        print 'Feature: %s App: %s' % (seg, apps.pop())


  def load_rules(self):
    pass

  def classify(self, pkg, ruleType):
    pass
