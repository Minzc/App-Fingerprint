import sys


from nltk import FreqDist
from fp_growth import find_frequent_itemsets
from utils import loadfile
from utils import Relation


class FNode:
    def __init__(self, feature):
        self.feature = feature
        self.appCounter = FreqDist()

    def inc(self, app):
        self.appCounter.inc(app)

    def genRules(self):
        totalNum = sum(self.appCounter.values())
        # feature, app, support, confidence
        return [(self.feature, app, 1.0 * num, 1.0 * num / totalNum) for app, num in self.appCounter.items()]


class FPRuler:
    def __init__(self):
        # feature, app, host
        self.rulesSet = []


    def addRules(self, rules):
        tmprules = {}
        for feature, app, host in rules:
            tmprules.setdefault(host, {})
            tmprules[host][feature] = app
        self.rulesSet.append(tmprules)


    def classify(self, record):
        # TODO change it to map
        labelRsts = []
        for rulesID, rules in enumerate(self.rulesSet):
            rst = set()
            features = _get_record_f(record)
            for feature in features:
                if record.host in rules and feature in rules[record.host]:
                    rst.add(rules[record.host][feature])
            # TODO handle multiple predicted apps
            if len(rst) > 0:
                labelRsts.append(rst.pop())
            else:
                labelRsts.append(None)
        return labelRsts


def _get_record_f(record):
    features = filter(None, record.path.split('/'))
    queries = record.querys
    for k, vs in filter(None, queries.items()):
        if len(k) < 2:
            continue
        features.append(k)
        for v in vs:
            if len(v) < 2:
                continue
            features.append(v.replace(' ', '').replace('\n', ''))

    for head_seg in filter(None, record.add_header.split('\n')):
        if len(head_seg) < 2:
            continue
        features.append(head_seg.replace(' ', '').strip())

    for agent_seg in filter(None, record.agent.split(' ')):
        if len(agent_seg) < 2:
            continue
        features.append(agent_seg.replace(' ', ''))

    features.append(record.host)
    return features


def _encode_data(records=None):
    if not records:
        records = load_pkgs(limit)
    train_data = []
    f_counter = FreqDist()
    f_company = Relation()

    for record in records:
        features = _get_record_f(record)

        recordVec = []
        for pathseg in features:
            f_counter.inc(pathseg)
            f_company.add(pathseg, record.company)

    valid_f = set()
    for k, v in f_counter.items():
        if v > 1 and len(f_company.get()[k]) < 4:
            valid_f.add(k)

    appIndx = {}
    featureIndx = {}
    f_indx = 0

    for record in records:
        features = _get_record_f(record)
        recordVec = []
        for pathseg in features:

            if pathseg not in valid_f:
                continue

            if pathseg not in featureIndx:
                f_indx += 1
                featureIndx[pathseg] = f_indx

            recordVec.append(featureIndx[pathseg])

        # if pathseg == 'petshop':
        # print 'DEBUG  OK', featureIndx[pathseg]

        host = record.host
        if not host:
            host = record.dst

        train_data.append(((record.app, record.host), sorted(set(recordVec), reverse=True)))


    # train_data
    # ((app, host), [f1, f2, f3])
    recordHost = []
    encodedRecords = []
    for record in train_data:
        if record[0][0] not in appIndx:
            f_indx += 1
            appIndx[record[0][0]] = f_indx
        # record[1].append(appIndx[record[0][0]])
        # encodedRecords.append(record[1])
        encodedRecords.append(({i for i in record[1]}, appIndx[record[0][0]]))
        recordHost.append(record[0][1])

    # encodedRecords: ({Features}, app)
    return encodedRecords, _rever_map(appIndx), _rever_map(featureIndx), recordHost


def _rever_map(mapObj):
    return {v: k for k, v in mapObj.items()}


def _load_data(filepath):
    records = []
    loadfile(filepath, lambda x: records.append(x.split(' ')))
    return records


def _load_appindx(filepath):
    apps = {}
    loadfile(filepath, lambda x: apps.setdefault(x.split('\t')[1], x.split('\t')[0]))
    return apps


def _load_findx(filepath):
    features = {}
    loadfile(filepath, lambda x: features.setdefault(x.split('\t')[1], x.split('\t')[0]))
    return features


def _load_records_hst(filepath):
    recordHost = []
    loadfile(filepath, lambda x: recordHost.append(x))
    return recordHost


def _gen_rules(transactions, tSupport, tConfidence, featureIndx):
    fNodes = {}
    ###########################
    # Apriori Version
    ###########################
    # apriori = Apriori(transactions)
    # fNodes = apriori.apriori(tSuppoert)

    ###########################
    # Single Item Version
    ###########################
    # for record in records:
    # # fts, app = record, record[-1]
    # fts, app = record
    # 	for ft in fts:
    # 		if ft not in fNodes:
    # 			fNodes[ft] = FNode(ft)
    # 		fNodes[ft].inc(app)

    ###########################
    # FP-tree Version
    ###########################
    find_frequent_itemsets(transactions, tSupport)
    rules = {}
    # for fNode in fNodes.values():
    #     for rule in fNode.genRules():
    #         feature, app, support, confidence = rule
    #         if support > tSupport and confidence > tConfidence:
    #             rules[feature] = (app, support, confidence)
    return rules


def _prune_rules(rules, records):
    finalRules = set()

    for hstindx, record in enumerate(records):
        for feature in record[0]:
            if frozenset([feature]) in rules:
                finalRules.add((feature, record[1], hstindx))
    return finalRules


def mine_fp(records, tSuppoert, tConfidence):
    ################################################
    # Mine App Features
    ################################################

    encodedRecords, appIndx, featureIndx, recordHost = _encode_data(records)
    # (feature, app, host index)
    rules = _gen_rules(encodedRecords, tSuppoert, tConfidence, _rever_map(featureIndx))
    # feature, app, host
    # rules : set()
    rules = _prune_rules(rules, encodedRecords)
    # change encoded features back to string
    decodedRules = set()
    for rule in rules:
        featureIndx[rule[0]]
        appIndx[rule[1]]
        recordHost[rule[2]]
        decodedRules.add((featureIndx[rule[0]], appIndx[rule[1]], recordHost[rule[2]]))

    classifier = FPRuler()
    classifier.addRules(decodedRules)
    ################################################
    # Mine Company Features
    ################################################


    for record in records:
        if record.company:
            record.app = record.company
    encodedRecords, appIndx, featureIndx, recordHost = _encode_data(records)
    rules = _gen_rules(encodedRecords, tSuppoert, tConfidence, _rever_map(featureIndx))
    rules = _prune_rules(rules, encodedRecords)
    decodedRules = set()
    for rule in rules:
        featureIndx[rule[0]]
        appIndx[rule[1]]
        recordHost[rule[2]]
        decodedRules.add((featureIndx[rule[0]], appIndx[rule[1]], recordHost[rule[2]]))
    classifier.addRules(decodedRules)
    return classifier


def mining_fp_local(filepath, tSuppoert, tConfidence):
    records = _load_data(filepath)
    appIndx = _load_appindx('app_index.txt')
    featureIndx = _load_findx('featureIndx.txt')
    recordHost = _load_records_hst('records_host.txt')

    # (feature, app, host index)
    rules = _gen_rules(records, tSuppoert, tConfidence)

    # feature, app, host
    rules = _prune_rules(rules, records)

    coverage = 0
    totalApp = set()
    for indx, record in enumerate(records):
        totalApp.add(record[-1])
        for feature in record[:-1]:
            if feature in rules:
                coverage += 1
                break
    coveredApp = set()
    for rule in rules:
        coveredApp.add(rule[1])
        rule = (featureIndx[rule[0]], appIndx[rule[1]], recordHost[rule[2]])
        print rule

    print 'total:', len(records), 'coverage:', coverage, 'totalApp:', len(totalApp), 'coverApp:', len(coveredApp)


def revers():
    appIndx = load_appindx('app_index.txt')
    featureIndx = load_findx('featureIndx.txt')


class Apriori:
    def __init__(self, records):
        # encodedRecords: ({Features}, app)
        self.records = records

    def _scanD(self, candidates, tSupport):
        fNodes = {}
        for record in self.records:
            features, app = record
            for can in candidates:
                if can.issubset(features):
                    fNodes.setdefault(can, FNode(can))
                    fNodes[can].inc(app)

        retlist = {}
        support_data = {}
        for freqSet, fNode in fNodes.items():
            if fNode.appCounter.max() >= tSupport:
                retlist[freqSet] = fNode
        return retlist

    def _genC1(self):
        C1 = []
        for record in self.records:
            for i in record[0]:
                C1.append([i])
        return map(frozenset, C1)

    def _aprioriGen(self, freqSets, k):
        retList = []
        lenLk = len(freqSets)
        for i in range(lenLk):
            for j in range(i + 1, lenLk):
                combinedSet = freqSets[i] | freqSets[j]
                if len(combinedSet) == k:
                    retList.append(combinedSet)
        return retList

    def apriori(self, minSupport):
        C1 = self._genC1()
        # {set(): FNode}
        L1 = self._scanD(C1, minSupport)
        L = [L1]

        k = 2
        while (len(L[k - 2]) > 0 and k < 2):
            Ck = self._aprioriGen(L[k - 2].keys(), k)
            Lk = self._scanD(Ck, minSupport)
            L.append(Lk)
            k += 1
        rst = {}
        for i in L:
            rst.update(i)
        return rst


if __name__ == '__main__':
    if sys.argv[1] == 'mine':
        mining_fp_local(sys.argv[2], tSuppoert=int(sys.argv[3]), tConfidence=float(sys.argv[4]))
	

