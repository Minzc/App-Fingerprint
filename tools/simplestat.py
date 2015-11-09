# -*- coding:utf-8 -*-
import sys

from utils import  load_pkgs
from sqldao import SqlDao
from collections import defaultdict
from const import consts

tbls = [ 'ios_packages_2015_09_14','ios_packages_2015_08_10', 'ios_packages_2015_06_08', 'ios_packages_2015_08_12', 'ios_packages_2015_08_04']
def stat_post_content():
    records = { tbl : load_pkgs(tbl, consts.IOS, limit = None) for tbl in tbls }
    features = defaultdict(lambda : defaultdict(lambda : set()))
    for tbl in records:
        for pkg in records[tbl]:
            if pkg.content:
                for lnseg in pkg.content.split('\n'):
                    lnseg = lnseg.strip()
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



if __name__ == '__main__':
    stat_post_content()
