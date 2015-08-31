import sys
import os
sys.path.append(os.getcwd())
from classifiers.algo import KVClassifier
from classifiers.fp import CMAR
from classifiers.utils import load_pkgs
from classifiers.host import HostApp
from classifiers.text_classifier import TextClassifier
from utils import load_exp_app

LIMIT = None
tbls = ['ios_packages_2015_08_12', 'ios_packages_2015_08_10', 'ios_packages_2015_08_04', 'ios_packages_2015_06_08']

def load_train_data():
  def keep_exp_app(package):
    return package.app in expApp[appType]
  expApp = load_exp_app()
  records = {}
  for tbl in train_tbls:
    records[tbl] = load_pkgs(limit = LIMIT, filterFunc = keep_exp_app, DB = tbl, appType = appType)
  return records

def test_text_classifier():
  textClassifier = TextClassifier()
  textClassifier.train()

test_text_classifier()
