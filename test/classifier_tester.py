from algo import KVClassifier
from fp import CMAR, FPRuler
from utils import load_pkgs
from host import HostApp
import consts

LIMIT = None

def load_data():
  train_tbls = ['packages_20150210','packages_20150509','packages_20150526']
  test_tbl = 'packages_20150429'

  expApp = {app.strip().lower() for app in open("resource/exp_app.txt")}
  records = {}

  for tbl in train_tbls:
    records[tbl] = load_pkgs(LIMIT, filterFunc = lambda x: x.app in expApp , DB = tbl)

  test_set = {record.id:record for record in load_pkgs(LIMIT, filterFunc = lambda x: x.app in expApp , DB = test_tbl)}
  return records, test_set

def use_classifier(classifier, test_set):
  rst = {}
  correct = 0
  recall = 0
  total = len(test_set)
  correct_company = 0
  correct_app = 0
  for pkg_id, record in test_set.items():
    labelDists = classifier.classify(record)
    # if consts.APP_RULE in labelDists and consts.COMPANY_RULE not in labelDists:
    #   print 'App', record.app
    #   print 'Queries', record.queries
    #   print 'Label', labelDists
    #   print '#' * 10
    for prediction in labelDists.values():
      if prediction[0] != None:
        recall += 1
        break
    
    if consts.APP_RULE in labelDists and labelDists[consts.APP_RULE][0] == record.app:
      correct += 1
      correct_app += 1
    elif consts.COMPANY_RULE in labelDists and labelDists[consts.COMPANY_RULE][0] == record.company:
      correct += 1
      correct_company += 1
  
  print 'Correct', correct, 'App', correct_app, 'Company', correct_company, 'Recall', recall

  return rst

def test_classifier():
  train_set, test_set = load_data()
  classifier_one = KVClassifier()
  for rule_type in [consts.COMPANY_RULE, consts.APP_RULE]:
    for tbl in train_set:
      for pkg in train_set[tbl]:
        if rule_type == consts.APP_RULE:
          pkg.set_label(pkg.app)
        elif rule_type == consts.COMPANY_RULE:
          pkg.set_label(pkg.company)
        elif rule_type == consts.CATEGORY_RULE:
          pkg.set_label(pkg.category)

    classifier_one = classifier_one.train(train_set, rule_type)

  classifier_one = KVClassifier()
  classifier_one.load_rules()
  
  use_classifier(classifier_one, test_set)

test_classifier()
