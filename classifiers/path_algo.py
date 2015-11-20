from utils import  flatten
from utils import load_xml_features
from utils import  url_clean, load_exp_app
from sqldao import SqlDao
from collections import defaultdict
import const.consts as consts
from const.app_info import AppInfos
from classifier import AbsClassifer
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




test_str = {'stats.3sidedcube.com', 'redcross.com'}


class PathApp(AbsClassifer):
    def __init__(self, appType):
        self.appType = appType
        self.pathLabel = defaultdict(set)
        self.substrCompany = defaultdict(set)
        self.labelAppInfo = {}
        self.rules = defaultdict(lambda : defaultdict(lambda : defaultdict( lambda : defaultdict(set))))
        self.xmlFeatures = load_xml_features()

    def _persist(self, rules, rule_type):
        '''specificRules[rule.host][ruleStrSet][label][consts.SCORE] = rule.support'''
        sqldao = SqlDao()
        QUERY = consts.SQL_INSERT_CMAR_RULES
        params = []
        for ruleType in rules:
            for pathSeg, label, host, tbls in flatten(rules[ruleType]):
                for label, scores in rules[ruleType][host][pathSeg].items():
                    params.append((label, pathSeg, 1, len(tbls), host, ruleType))
            sqldao.executeBatch(QUERY, params)
            sqldao.close()
            print "Total Number of Rules is", len(rules)

    def _check(self, url, label):
        for feature in self.fLib[label]:
            if feature in url:
                return True
        return False

    def count(self, pkg):
        features = self._get_package_f(pkg)

        self.labelAppInfo[pkg.label] = [pkg.website]
        map(lambda pathSeg: self.pathLabel[pathSeg].add(pkg.label), features)

    @staticmethod
    def _clean_db(rule_type):
        QUERY = consts.SQL_DELETE_HOST_RULES
        sqldao = SqlDao()
        sqldao.execute(QUERY % rule_type)
        sqldao.close()

    def _feature_lib(self, expApp):
        self.fLib = defaultdict(set)
        segApps = defaultdict(set)
        for label, appInfo in expApp.iteritems():
            appSegs = appInfo.package.split('.')
            companySegs = appInfo.company.split(' ')
            categorySegs = appInfo.category.split(' ')
            websiteSegs = url_clean(appInfo.website).split('.')
            valueSegs = set()
            for _, value in self.xmlFeatures[label]:
                valueSegs |= set(value.split(' '))

            wholeSegs = [appSegs, companySegs, categorySegs, websiteSegs, valueSegs]

            for segs in wholeSegs:
                for seg in segs:
                    self.fLib[label].add(seg)
                    segApps[seg].add(label)
        for label, segs in self.fLib.items():
            self.fLib[label] = {seg for seg in segs if len(segApps[seg]) == 1}

    def _get_package_f(self, package):
        """Get package features"""
        features = filter(None, map(lambda x: x.strip(), package.path.split('/')))
        return features

    def train(self, records, rule_type):
        expApp = load_exp_app()[self.appType]
        expApp = {label: AppInfos.get(self.appType, label) for label in expApp}
        self._feature_lib(expApp)
        for pkgs in records.values():
            for pkg in pkgs:
                self.count(pkg)
        ########################
        # Generate Rules
        ########################

        rules = defaultdict(lambda : defaultdict())

        for pathSeg, labels in self.pathLabel.iteritems():
            if pathSeg in test_str:
                print '#', len(labels)
                print labels
                print pathSeg

            if len(labels) == 1:
                label = list(labels)[0]
                ifValidRule = self._check(pathSeg, label)

                if ifValidRule:
                    rules[rule_type][pathSeg] = label

                if pathSeg in test_str:
                    print 'Rule Type is', rule_type, ifValidRule, pathSeg

        print 'number of rule', len(self.rules[consts.APP_RULE])

        self.count_support(rules, records)
        self._persist(self.rules,  rule_type)
        self.__init__(self.appType)
        return self

    def load_rules(self):
        self.rules = {consts.APP_RULE: {}, consts.COMPANY_RULE: {}, consts.CATEGORY_RULE: {}}
        QUERY = consts.SQL_SELECT_HOST_RULES
        sqldao = SqlDao()
        counter = 0
        for host, label, ruleType, support in sqldao.execute(QUERY):
            counter += 1
            regexObj = re.compile(r'\b' + re.escape(host) + r'\b')
            self.rules[ruleType][host] = (label, support, regexObj)
        print '>>> [Host Rules#loadRules] total number of rules is', counter, 'Type of Rules', len(self.rules)
        sqldao.close()

    def count_support(self, rules, records):
        for tbl, pkgs in records.items():
            for pkg in pkgs:
                for ruleType in rules:
                    for feature in self._get_package_f(pkg):
                        if feature in rules[ruleType] and pkg.app in rules[ruleType][feature]:
                            self.rules[ruleType][feature][pkg.app][pkg.host].add(tbl)


    def _recount(self, records):
        for tbl, pkgs in records.items():
            for pkg in pkgs:
                for url, labels in self.pathLabel.iteritems():
                    if len(labels) == 1 and (url in pkg.host or url in pkg.refer_host):
                        self.pathLabel[url].add(pkg.label)

    def classify(self, pkg):
        """
        Input
        - self.rules : {ruleType: {host : (label, support, regexObj)}}
        :param pkg: http packet
        """
        rst = {}
        for ruleType in self.rules:
            predict = consts.NULLPrediction
            for regexStr, ruleTuple in self.rules[ruleType].iteritems():
                label, support, regexObj = ruleTuple
                #host = pkg.refer_host if pkg.refer_host else pkg.host
                host = pkg.rawHost
                match = regexObj.search(host)
                if match and predict.score < support:
                    if match.start() == 0:
                        predict = consts.Prediction(label, support, (host, regexStr, support))

                    # if pkg.app == 'com.logos.vyrso' and pkg.host == 'gsp1.apple.com':
                    #   print regexStr
                    # print match

            rst[ruleType] = predict
            if predict.label != pkg.app and predict.label is not None:
                print 'Evidence:', predict.evidence, 'App:', pkg.app, 'Predict:', predict.label
        return rst

if __name__ == '__main__':
    host_tree()
