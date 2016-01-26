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
    def __init__(self, tbls, rmapp):
        self.tables = tbls
        self.__data = {}
        self.apps = set()
        self.rmapp = rmapp

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
    def get_traindata(tbls, appType):
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
            return package.app in sampledApps

        print '[TRAIN FACTORY] Loading data set', tbls, 'SAMPLE RATE is', conf.sample_rate
        expApp = load_exp_app()[appType]

        # Do sample
        print '[TRAIN FACTORY] Before Sample', len(expApp)
        sampledApps = {app for app in expApp if random.uniform(0, 1) <= conf.sample_rate}
        rmApps = expApp - sampledApps
        print '[TRAIN FACTORY] After Sample', len(sampledApps)

        dataSet = DataSet(tbls, rmApps)

        for tbl in tbls:
            pkgs = load_pkgs(limit=conf.package_limit, filterFunc=_keep_exp_app, DB=tbl, appType=appType)
            dataSet.set_data(tbl, pkgs)


        assert (len(rmApps) + len(sampledApps) == len(expApp), 'Sampling method is wrong')
        assert (dataSet.check(), "Did not load enough data")

        return dataSet
