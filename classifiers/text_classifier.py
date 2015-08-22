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
    for app in appInfos[consts.IOS]:
      pkgSegs = app.package.split('.')
      count(pkgSegs)
      nameSegs = app.name.split(' ')
      count(nameSegs)
      companySegs = app.company.split(' ')
    
    rules = 



