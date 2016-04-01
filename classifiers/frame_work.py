# -*- encoding = utf-8 -*-
from abc import abstractmethod, ABCMeta

import const.sql
from const import conf
from utils import flatten, process_agent
from sqldao import SqlDao
from collections import defaultdict
import const.consts as consts
from classifier import AbsClassifer
import re
import urllib
from const.dataset import DataSetIter as DataSetIter

SPLITTER = re.compile("[" + r'''!"#$%&'()*+,\-/:;<=>?@[\]^_`{|}~ ''' + "]")

def process_agent(agent, app):
    agent = re.sub(r'[a-z]?[0-9]+-[a-z]?[0-9]+-[a-z]?[0-9]+', r'[VERSION]', agent)
    agent = re.sub(r'(/)([0-9]+)([ ;])', r'\1[VERSION]\3', agent)
    agent = re.sub(r'/[0-9][.0-9]+', r'/[VERSION]', agent)
    agent = re.sub(r'([ :v])([0-9][.0-9]+)([ ;),])', r'\1[VERSION]\3', agent)
    agent = re.sub(r'([ :v])([0-9][_0-9]+)([ ;),])', r'\1[VERSION]\3', agent)
    agent = re.sub(r'(^[0-9a-z]*)(.'+app+r'$)', r'[RANDOM]\2', agent)
    return agent
class FrameWork:
    __metaclass__ = ABCMeta
    @abstractmethod
    def reformat(self, trainSet):
        return

class Agent(FrameWork):

    def reformat(self, trainSet):
        for tbl, pkg in DataSetIter.iter_pkg(trainSet):
            agent = process_agent(pkg.agent, pkg.app)
            words = SPLITTER.split(data)
        return words

    def cal_corr(self, trainData, threshold):
        labels = defaultdict(int)
        features = defaultdict(int)
        cooccur = defaultdict(int)
        D = 0
        for tbl, tps in trainData.items():
            D += 1
            for app, agent, host in tps:
                words = SPLITTER.split(agent)
                # print(agent, words)
                for word in set(words):
                    word = word.strip()
                    if len(word) > 1:
                        features[word] += 1
                        cooccur[(app, word)] +=1
                labels[app] += 1

        corr = defaultdict(set)
        unticorr = defaultdict(set)
        for appWord, count in cooccur.items():
            app, word = appWord
            value = ( count * D * 1.0 ) / (labels[app] * features[word])
            #print(app, word, value)
            if value > threshold:
                corr[app].add(word)
            else:
                unticorr[app].add(word)
        return corr, unticorr