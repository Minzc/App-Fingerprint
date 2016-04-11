# -*- encoding = utf-8 -*-
from __future__ import absolute_import, division, print_function, unicode_literals
from abc import abstractmethod, ABCMeta

from utils import process_agent
from collections import defaultdict
import const.consts as consts
import re
from const.dataset import DataSetIter as DataSetIter, DataSetFactory

SPLITTER = re.compile("[" + r'''!"#$%&'()*+,\-/:;<=>?@[\]^_`{|}~ ''' + "]")

class FrameWork:
    __metaclass__ = ABCMeta
    @abstractmethod
    def reformat(self, trainSet):
        return
    def score(self, trainSet):
        return

class Agent(FrameWork):
    def reformat(self, trainSet):
        trainData = set()
        for tbl, pkg in DataSetIter.iter_pkg(trainSet):
            trainData.add((
                pkg.app,
                tuple(['^'] + filter(lambda item: len(item) > 1,
                       map(lambda item: item.strip(),
                            SPLITTER.split(process_agent(pkg.agent, pkg.app)))) + ['$']),
                pkg.host,
                tbl
            ))
        return trainData

    def find_frequent_context(self, trainData):
        corr, stopwords = self.cal_idf(trainData, 0.7)
        trainSet = set()
        for app, items, host, tbl in trainData:
            startIndex = -1
            for i, word in enumerate(items):
                if startIndex == -1 and word in corr:
                    startIndex = i-1
                if startIndex != -1 and word not in corr:
                    endIndex = i
                    print('#', items, (items[startIndex], items[endIndex], items[startIndex: endIndex]))
                    trainSet.add(
                        (tbl, host, (items[startIndex], items[endIndex]), items[startIndex + 1 : endIndex])
                    )
                    startIndex = -1
        return trainSet

    def cal_idf(self, trainData, threshold):
        appCounter = defaultdict(int)
        features = defaultdict(int)
        cooccur = defaultdict(set)
        for app, items, host, tbl in trainData:
            for item in set(items):
                features[item] += 1
                cooccur[item].add(app)
            appCounter[app] += 1
        corr = set()
        stopwords = set()
        for item, apps in cooccur.items():
            value = len(apps) / len(appCounter)
            if value < threshold:
                print(item, value)
                corr.add(item)
            else:
                stopwords.add(item)
        return corr, stopwords

trainTbls = ['ios_packages_2015_09_14', 'ios_packages_2015_08_10']
trainSet = DataSetFactory.get_traindata(tbls=trainTbls, appType=consts.IOS)
agent = Agent()
trainSet = agent.reformat(trainSet)
for tbl, host, key, value in trainSet:
    print('>>>', tbl, host, key, value)