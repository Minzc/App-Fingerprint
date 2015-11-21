from utils import flatten
from utils import load_xml_features
from utils import url_clean, load_exp_app
from sqldao import SqlDao
from collections import defaultdict
import const.consts as consts
from const.app_info import AppInfos
from classifier import AbsClassifer
from const.dataset import DataSetIter as DataSetIter
import re

# class TreeNode:
#     def __init__(self, father, value):
#         self.children = []
#         self.father = [father]
#         self.value = value
#         self.status = 1
#         self.counter = 0
#         self.leaf = {}
#         self.label = FreqDist()
#
#     def inc_label(self, label):
#         self.label.inc(label)
#
#     def inc_counter(self):
#         self.counter += 1
#
#     def get_counter(self):
#         return self.counter
#
#     def get_value(self):
#         return self.value
#
#     def add_child(self, treenode):
#         self.children.append(treenode)
#
#     def get_child(self, value):
#         for child in self.children:
#             if child.value == value:
#                 return child
#         return None
#
#     def get_all_child(self):
#         return self.children
#
#     def get_father(self):
#         return self.father
#
#     def set_status(self, status):
#         self.status = status
#
#     def get_status(self):
#         return self.status
#
#     def to_string(self):
#         return ','.join([child.get_value() for child in self.children])
#
#     def add_leaf(self, node):
#         self.leaf[node.get_value()] = node
#
#     def get_all_leaf(self):
#         return self.leaf
#
#     def add_father(self, node):
#         self.father.append(node)
#
#
# def _add_node(root, tree_path, label):
#     tree_node = root
#
#     for i in range(len(tree_path)):
#         node_value = tree_path[i]
#         child_node = tree_node.get_child(node_value)
#         # not adding leaf node
#         if child_node == None:
#             # not leaf node
#             child_node = TreeNode(tree_node, node_value)
#             tree_node.add_child(child_node)
#
#         child_node.inc_counter()
#         child_node.inc_label(label)
#         tree_node = child_node
#
#
# def host_tree(train_set=None):
#
#     root = TreeNode(None, None)
#     c = 0
#
#     if not train_set:
#         train_set = _get_train_set()
#
#     for package in train_set:
#         if not package.company or 'X-Requested-With' in package.add_header:
#             continue
#         tree_path = [package.host, package.app + '$' + package.company, package.path]
#         _add_node(root, tree_path, package.company)
#
#     QUERY = 'INSERT INTO rules (hst, path, company, app) VALUES (%s,%s,%s,%s)'
#     for hstnode in root.get_all_child():
#         companies = set()
#         packages = set()
#         for appnode in hstnode.get_all_child():
#             app, company = appnode.get_value().split('$')
#             companies.add(company)
#             packages.add(app_clean(app).split('.')[-1])
#
#         records = []
#         for appnode in hstnode.get_all_child():
#             for pathnode in appnode.get_all_child():
#                 for i in range(pathnode.get_counter()):
#                     features = [p for p in pathnode.get_value().split('/') if len(p) > 0]
#                     records.append([appnode.get_value(), features])
#         rules = pathtree(records, tfidf)
#         company = '$'.join(companies)
#
#         if (len(packages) == 1 or len(companies) == 1):
#             sqldao.execute(QUERY, (hstnode.get_value(), '', company, ''))
#         else:
#             maxlabel = hstnode.label.max()
#             if hstnode.label[maxlabel] * 1.0 / sum(hstnode.label.values()) >= 0.9:
#                 sqldao.execute(QUERY, (hstnode.get_value(), '', maxlabel, ''))
#
#         for f, app in rules.items():
#             app, company = app.split('$')
#             sqldao.execute(QUERY, (hstnode.get_value(), f, company, app))
#     sqldao.close()


# def pathtree(records, tfidf):
#     """
# 	[label, (feature1, feature2, feature3)]
# 	"""
#     root = TreeNode(None, None)
#     for record in records:
#         _add_node(root, record[1], record[0])
#
#     queue = root.get_all_child()
#     rules = {}
#
#     while len(queue):
#         node = queue[0]
#         queue = queue[1:]
#         sumv = sum(node.label.values())
#         maxlabel = node.label.max()
#         if node.label[maxlabel] == sumv:
#             app, company = maxlabel.split('$')
#             bestf = node.get_value()
#             score = tfidf[bestf][app]
#             while (len(node.get_all_child()) == 1):
#                 f = node.get_all_child()[0].get_value()
#                 if score < tfidf[f][app]:
#                     score = tfidf[f][app]
#                     bestf = f
#                 node = node.get_all_child()[0]
#             rules[bestf] = maxlabel
#         elif node.label[maxlabel] > sumv * 0.9:
#             rules[node.get_value()] = maxlabel
#         else:
#             for child in node.get_all_child():
#                 queue.append(child)
#     return rules




test_str = {'Market_RoyalFarms_001'.lower()}


class PathApp(AbsClassifer):
    def __init__(self, appType):
        self.appType = appType
        self.pathLabel = defaultdict(set)
        self.substrCompany = defaultdict(set)
        self.labelAppInfo = {}
        self.rules = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: defaultdict(set))))
        self.xmlFeatures = load_xml_features()

    def _persist(self, rules, rule_type):
        '''specificRules[rule.host][ruleStrSet][label][consts.SCORE] = rule.support'''
        sqldao = SqlDao()
        QUERY = consts.SQL_INSERT_CMAR_RULES
        params = []
        for ruleType in rules:
            print 'Length is', len(rules[ruleType])
            for pathSeg, label, host, tbls in flatten(rules[ruleType]):
                params.append((label, pathSeg, 1, len(tbls), host, ruleType))
            sqldao.executeBatch(QUERY, params)
            sqldao.close()
            print "Total Number of Rules is", len(rules[ruleType])

    def _check(self, url, label):
        for featureSet in self.fLib[label]:
            ifIn = True

            if type(featureSet) == tuple:
                for feature in featureSet:
                    if feature not in url:
                         ifIn = False
                    elif url in test_str:
                        print featureSet, url
            else:
                ifIn = featureSet in url

            if ifIn == True:
                if url in test_str:
                    print featureSet

                return True
        return False

    def count(self, pkg):
        features = self._get_package_f(pkg)
        map(lambda pathSeg: self.pathLabel[pathSeg].add(pkg.label), features)

    @staticmethod
    def _clean_db(rule_type):
        QUERY = consts.SQL_DELETE_HOST_RULES
        sqldao = SqlDao()
        sqldao.execute(QUERY % rule_type)
        sqldao.close()

    def _feature_lib(self, expApp):
        def _getitemset(fSet):
            itemset = filter(lambda x: len(x)> 1, fSet)
            itemset += [(itemset[i], itemset[i+1]) for i in range(0, len(itemset)-1)]
            return itemset

        self.fLib = defaultdict(set)
        segApps = defaultdict(set)
        for label, appInfo in expApp.iteritems():
            appSegs = appInfo.package.split('.')
            appSegs = _getitemset(appSegs)

            companySegs = appInfo.company.split(' ')
            companySegs = _getitemset(companySegs)

            nameSegs = appInfo.name.split(' ')
            nameSegs = _getitemset(nameSegs)

            categorySegs = appInfo.category.split(' ')

            websiteSegs = url_clean(appInfo.website).split('.')

            valueSegs = set()
            for _, value in self.xmlFeatures[label]:
                valueSegs |= set(value.split(' '))

            wholeSegs = [appSegs, companySegs, categorySegs, websiteSegs, valueSegs, nameSegs]

            for segs in wholeSegs:
                for seg in segs:
                    self.fLib[label].add(seg)
                    segApps[seg].add(label)


        for label, segs in self.fLib.items():
            self.fLib[label] = {seg for seg in segs if len(segApps[seg]) == 1 and len(seg) > 1}

        print self.fLib['com.vergeretail.marketroyalfarms']
        # print self.fLib['com.dci.blackenterprise']
        # print 'Enterprise'.lower() in self.fLib['com.iphonehyatt.prod']

    @staticmethod
    def _get_package_f(package):
        """Get package features"""
        features = filter(None, map(lambda x: x.strip(), package.path.split('/')))
        for feature in features:
            if feature in test_str:
                print 'OK!!!'
        return features

    def train(self, trainData, rule_type):
        expApp = load_exp_app()[self.appType]
        expApp = {label: AppInfos.get(self.appType, label) for label in expApp}
        self._feature_lib(expApp)
        for tbl, pkg in DataSetIter.iter_pkg(trainData):
            self.count(pkg)
        ########################
        # Generate Rules
        ########################

        rules = defaultdict(dict)

        for pathSeg, labels in self.pathLabel.iteritems():
            if pathSeg in test_str:
                print '#', len(labels)
                print labels
                print pathSeg

            if len(labels) == 1:
                label = list(labels)[0]
                ifValidRule = self._check(pathSeg, label)

                if pathSeg in test_str:
                    print ifValidRule, pathSeg in self.fLib[label], label

                if ifValidRule:
                    rules[rule_type][pathSeg] = label

                if pathSeg in test_str:
                    print 'Rule Type is', rule_type, ifValidRule, pathSeg

        print 'number of rule', len(rules[consts.APP_RULE])

        print rules
        self.count_support(rules, trainData)
        self._persist(self.rules, rule_type)
        self.__init__(self.appType)
        return self

    def load_rules(self):
        self.rules = {}
        self.rules[consts.APP_RULE] = defaultdict(lambda: defaultdict())
        self.rules[consts.COMPANY_RULE] = defaultdict(lambda: defaultdict())
        self.rules[consts.CATEGORY_RULE] = defaultdict(lambda: defaultdict())
        sqldao = SqlDao()
        counter = 0
        SQL = consts.SQL_SELECT_CMAR_RULES
        for label, patterns, host, ruleType, support in sqldao.execute(SQL):
            counter += 1
            patterns = frozenset(map(lambda x: x.strip(), patterns.split(",")))
            self.rules[ruleType][host][patterns] = (label, support)
        sqldao.close()
        print '>>>[CMAR] Totaly number of rules is', counter
        for ruleType in self.rules:
            print '>>>[CMAR] Rule Type %s Number of Rules %s' % (ruleType, len(self.rules[ruleType]))

    def count_support(self, rules, trainData):
        for tbl, pkg in DataSetIter.iter_pkg(trainData):
            for ruleType in rules:
                for feature in self._get_package_f(pkg):
                    if feature in rules[ruleType] and pkg.app == rules[ruleType][feature]:
                        self.rules[ruleType][feature][pkg.app][pkg.host].add(tbl)
                    elif feature in rules[ruleType] and pkg.app != rules[ruleType][feature]:
                        print rules[ruleType][feature]


    def classify(self, package):
        '''
        Return {type:[(label, confidence)]}
        '''
        labelRsts = {}
        features = self._get_package_f(package)
        for rule_type, rules in self.rules.iteritems():
            rst = consts.NULLPrediction
            max_confidence = 0
            if package.host in rules.keys():
                for rule, label_confidence in rules[package.host].iteritems():
                    label, confidence = label_confidence
                    if rule.issubset(features):  # and confidence > max_confidence:
                        max_confidence = confidence
                        rst = consts.Prediction(label, confidence, rule)

            labelRsts[rule_type] = rst
            if rule_type == consts.APP_RULE and rst != consts.NULLPrediction and rst.label != package.app:
                print rst, package.app
                print '=' * 10
        return labelRsts


