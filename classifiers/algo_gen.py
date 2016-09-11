#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import absolute_import, division, print_function, unicode_literals
import random
import re
from collections import defaultdict
import datetime
import const.consts as consts
from const import conf
from const.dataset import DataSetIter as DataSetIter, DataSetFactory
from utils import if_version

DEBUG = False


def print(*args, **kwargs):
    return __builtins__.print(*tuple(['[%s]' % str(datetime.datetime.now())] + list(args)), **kwargs)



def get_f(package):
    def filter_func(v):
        v = v.strip()
        return if_version(v) == False and len(v) > 1

    host = re.sub('[0-9]+\.', '[0-9]+.', package.rawHost)
    queries = package.queries
    for k, vs in queries.items():
        k = k.replace("\t", "")
        for v in filter(filter_func, vs):
            v = re.sub('[0-9]+x[0-9]+', '', v.strip())
            if '/' in v:
                v = v.split('/')[0]
            yield (host, k, v)


FC = set()


class QueryClassifier():
    def __init__(self, appType):
        self.name = consts.KV_CLASSIFIER
        self.Rules = defaultdict(list)
        self.appType = appType

    def train(self, trainData, ruleType):
        for datasize in [200000, 300000, 400000, 500000, 600000, 700000]:
            HDB = [(tbl, pkg) for tbl, pkg in DataSetIter.iter_pkg(trainData)]
            HDB = [HDB[i] for i in sorted(random.sample(xrange(len(HDB)), datasize))]
            print('Datasize:', len(HDB))
            groups = defaultdict(list)
            for tbl, pkg in HDB:
                for host, key, value in get_f(pkg):
                    groups[host].append((pkg.trackId, tbl, key, value, pkg.app))

            print('Start mining frequent contexts')
            for host in groups:
                contexts = defaultdict(set)
                omega = set()
                seqs = defaultdict(set)
                sigAppMap = defaultdict(set); appSigMap = defaultdict(set); SC = defaultdict(set); date = defaultdict(set)
                for id, tbl, context, sig, app in groups[host]:
                    omega.add(app)
                    contexts[context].add(app)
                    sigAppMap[sig].add(app)
                    appSigMap[app].add(sig)
                    SC[sig].add(id)
                    date[(sig, app, context)].add(tbl)
                    date[(app, context)].add(tbl)
                    seqs[context].add(sig)

                for context, apps in contexts.items():
                    support = len(apps) * 1.0 / len(omega)
                    if support > conf.query_labelT:
                        effective = 0
                        for sig in seqs[context]:
                            if len(sigAppMap[sig]) == 1:
                                app = list(sigAppMap[sig])[0]
                                rel = 1.0 / len(appSigMap[app])
                                dateScore = 1.0 * len(date[(sig, app, context)]) / len(date[(app, context)])
                                effective += rel * dateScore
                        effective = effective / len(sigAppMap)
                        quality = 2 * effective * support / (effective + support)
                        if quality > conf.query_scoreT:
                            for sig in seqs[context]:
                                if len(sigAppMap[sig]) == 1:
                                    app = list(sigAppMap[sig])[0]
                                    rel = 1.0 / len(appSigMap[app])
                                    dateScore = 1.0 * len(date[(sig, app, context)]) / len(date[(app, context)])
                                    sigQ = rel * dateScore
                                    for id in SC[sig]:
                                        self.Rules[id].append((quality, sigQ, consts, host, sig))
                                        self.Rules[i] = sorted(self.Rules[i], key=lambda x: (x[1], x[2]), reverse=True)[:conf.query_K]



if __name__ == '__main__':
    tbls = ['ca_ios_packages_2015_12_10', 'ca_ios_packages_2015_05_29', 'ca_ios_packages_2016_02_22',
            'chi_ios_packages_2015_07_20', 'chi_ios_packages_2015_09_24', 'chi_ios_packages_2015_09_24',
            'ios_packages_2015_06_08','ios_packages_2015_08_04']

    trainSet = DataSetFactory.get_traindata(tbls=tbls, appType=consts.IOS)
    a = QueryClassifier(consts.APP_RULE)
    a.train(trainSet, consts.IOS)
    print('Finish')
