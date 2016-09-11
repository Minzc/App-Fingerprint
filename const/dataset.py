from collections import defaultdict

from const import conf
from utils import load_pkgs, load_exp_app
import random
import const.consts as consts


class DataSetIter:
    def __init__(self):
        pass

    @staticmethod
    def iter_kv(dataset):
        data = dataset.get_data()
        for tbl in data.keys():
            for pkg in data[tbl]:
                host, queries = pkg.host, pkg.queries
                for k, vs in queries.items():
                    for v in vs:
                        yield (tbl, pkg, k, v)

    @staticmethod
    def iter_pkg(dataset):
        data = dataset.get_data()
        for tbl in data.keys():
            for pkg in data[tbl]:
                yield (tbl, pkg)


class DataSet:
    def __init__(self, tbls):
        self.tables = tbls
        self.__data = {}
        self.apps = set()

    def set_data(self, tbl, data):
        self.__data[tbl] = data
        for pkg in data:
            self.apps.add(pkg.app)

    def check(self):
        len(self.tables) == len(self.__data)

    def set_label(self, ruleType):
        for tbl in self.__data:
            for pkg in self.__data[tbl]:
                if ruleType == consts.APP_RULE:
                    pkg.set_label(pkg.app)
                elif ruleType == consts.COMPANY_RULE:
                    pkg.set_label(pkg.company)
                elif ruleType == consts.CATEGORY_RULE:
                    pkg.set_label(pkg.category)

    def get_data(self):
        return self.__data

    def get_size(self):
        return {k:len(v) for k, v in self.__data.items()}


class DataSetFactory:
    def __init__(self):
        pass

    @staticmethod
    def get_traindata(tbls, appType, sampledApps = set()):
        """
        Load data from given table
        :param sampleRate:
        :param tbls: a list of tables
        :param appType: IOS or ANDROID
        :param LIMIT:

        Output
        - record : {table_name : [list of packages]}

        """

        def _keep_exp_app(package):
            return package.app in expApp

        print '[TRAIN FACTORY] Loading data set', tbls, 'SAMPLE RATE is', conf.sample_rate
        expApp = load_exp_app()[appType]

        # Do sample
        print '[TRAIN FACTORY] Before Sample', len(expApp)
        if len(sampledApps) != 0:
            expApp =sampledApps
        print '[TRAIN FACTORY] After Sample', len(expApp)



        dataSet = DataSet(tbls)
        pkgs = []

        for tbl in tbls:
            pkgs += [(tbl, pkg) for pkg in load_pkgs(limit=conf.package_limit, filterFunc=_keep_exp_app, DB=tbl, appType=appType)]


        tblPkgs = defaultdict(list)
        if len(sampledApps) == 0:
            print(len(pkgs))
            pkgs = {pkgs[i] for i in sorted(random.sample(xrange(len(pkgs)), conf.sample_rate))}
        for pkg in pkgs:
            tblPkgs[pkg[0]].append(pkg[1])
        for tbl, pkgs in tblPkgs.items():
            dataSet.set_data(tbl, pkgs)


        assert (dataSet.check(), "Did not load enough data")

        return dataSet
