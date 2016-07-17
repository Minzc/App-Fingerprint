# -*- coding:utf-8 -*-
import sys

import re

import utils
from classifiers.algo import KV, Path, Head
from const.dataset import DataSetFactory
from utils import  load_pkgs, load_xml_features
from sqldao import SqlDao
from collections import defaultdict
from const import consts
import urllib

tbls = [ 'ios_packages_2015_09_14','ios_packages_2015_08_10', 'ios_packages_2015_06_08', 'ios_packages_2015_08_12', 'ios_packages_2015_08_04']
VALID_FEATURES = {'CFBundleName', 'CFBundleExecutable', 'CFBundleIdentifier', 'CFBundleDisplayName', 'CFBundleURLSchemes'}
def stat_post_content():
    records = { tbl : load_pkgs(tbl, consts.IOS, limit = None) for tbl in tbls }
    features = defaultdict(lambda : defaultdict(lambda : set()))
    xmlFeatures = load_xml_features()
    for tbl in records:
        for pkg in records[tbl]:
            if pkg.content:
                path = pkg.content
                path = urllib.unquote(path).lower().replace(';', '?', 1).replace(';', '&')
                for lnseg in path.split('\n'):
                    lnseg = lnseg.strip()
                    for valueTuples in xmlFeatures[pkg.app]:
                        fieldName, fieldValue = valueTuples
                        if fieldValue in lnseg and fieldName in VALID_FEATURES:
                            features[lnseg][pkg.app].add(tbl)

    for lnseg, apps in features.items():
        if len(apps) == 1:
            for app in apps:
                if len(features[lnseg][app]) > 1:
                    print app, lnseg, len(features[lnseg][app])


def stat_pos_content_kv():
    from classifiers.algo import QueryClassifier
    classifier = QueryClassifier(consts.IOS, inferFrmData = True, sampleRate = 1)
    classifier.load_rules()
    specificRules = classifier.rules[consts.APP_RULE]
    paramKeys = set()
    for host in specificRules:
        for key in specificRules[host]:
            paramKeys.add(key)
    records = { tbl : load_pkgs(tbl, consts.IOS, limit = None) for tbl in tbls }
    features = defaultdict(lambda : defaultdict(lambda : set()))
    for tbl in records:
        for pkg in records[tbl]:
            if pkg.content:
                path = pkg.content
                path = urllib.unquote(path).lower().replace(';', '?', 1).replace(';', '&')
                for lnseg in path.split('\n'):
                    lnseg = lnseg.strip()
                    for key in paramKeys:
                        if key in lnseg:
                            features[lnseg][pkg.app].add(tbl)

    for lnseg, apps in features.items():
        if len(apps) == 1:
            for app in apps:
                if len(features[lnseg][app]) > 1:
                    print app, lnseg, len(features[lnseg][app])

def findExpApps():
    QUERY = "SELECT DISTINCT(app) FROM %s WHERE method = \'GET\'"
    sqldao = SqlDao()
    apps = [set()]
    for tbl in tbls:
      apps.append(set())
      for app in sqldao.execute(QUERY % tbl):
          apps[0].add(app[0])
          apps[-1].add(app[0])

    rst = apps[0]
    for i in range(1, len(apps)):
      rst = rst & apps[i]

    print "len of app is", len(rst)
    for app in rst:
        print app

def rmOtherApp():
    def loadExpApp():
        expApp=set()
        for app in open("resource/exp_app.txt"):
            expApp.add(app.strip().lower())
        return expApp
    expApp = loadExpApp()
    apps = set()
    sqldao = SqlDao()
    for tbl in tbls:
      QUERY = "DELETE FROM " + tbl + " WHERE app=\'%s\'"
      for app in sqldao.execute("SELECT distinct app FROM %s" % tbl):
          apps.add(app[0])
      for app in apps:
          if app not in expApp:
              sqldao.execute(QUERY % (app))
              sqldao.commit()
    sqldao.close()

SPLITTER = re.compile("[" + r'''"#$%&*+,:<=>?@[\]^`{|}~ \-''' + "]")
def statData():
    tbls = ['ca_ios_packages_2015_12_10', 'ca_ios_packages_2015_05_29', 'ca_ios_packages_2016_02_22']

    trainSet = DataSetFactory.get_traindata(tbls=tbls, appType=consts.IOS)
    length = defaultdict(int)
    distinctItems = defaultdict(set)
    counter = defaultdict(int)
    kv = KV(0)
    head = Head(0)
    for tbl, pkg in trainSet:
        if pkg.agent != 'None':
            items = SPLITTER.split(utils.process_agent(pkg.agent))
            for item in items:
                distinctItems['agent'].add(item)
            length['agent'] += len(items)
            counter['agent'] += 1
        kvs = [(host, key, value) for (host, key, value) in kv.get_f(pkg)]
        if len(kvs) > 1:
            length['kv'] += len(kvs)
            counter['kv'] += 1
            for h, k, v in kvs:
                distinctItems['kv'].add(k)
                distinctItems['kv'].add(v)

        paths = filter(None, pkg.path.split('/'))
        if len(paths) > 1:
            length['path'] += len(paths)
            counter['path'] += 1
            for i in paths:
                distinctItems['path'].add(i)


        heads = [(host, key, value) for (host, key, value) in head.get_f(pkg)]
        if len(heads) > 1:
            length['head'] += len(heads)
            counter['head'] += 1
            for h, k, v in heads:
                distinctItems['head'].add(k)
                distinctItems['head'].add(v)
    print('[Agent] distinct items:', len(distinctItems['agent']), 'Average length:', length['agent'] * 1.0 / counter['agent'])
    print('[KV] distinct items:', len(distinctItems['kv']), 'Average length:', length['kv'] * 1.0 / counter['kv'])
    print('[Path] distinct items:', len(distinctItems['path']), 'Average length:', length['path'] * 1.0 / counter['path'])
    print('[Head] distinct items:', len(distinctItems['head']), 'Average length:', length['head'] * 1.0 / counter['head'])





if __name__ == '__main__':
    statData()
