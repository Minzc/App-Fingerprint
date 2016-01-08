# -*- utf-8 -*-

import sys
from sqldao import SqlDao
from collections import defaultdict

def stat_all():
    target = {'/pep/gcc':'gsp1.apple.com',
              '/assets/com_apple_MobileAsset_Duet/com_apple_MobileAsset_Duet.xml':'mesu.apple.com',
              '/configurations/pep/pipeline/pipeline0.html':'configuration.apple.com',
              '/bag':'init-p01st.push.apple.com'}
    tbls = ['ios_packages_2015_09_14', 'ios_packages_2015_08_10', 'ios_packages_2015_06_08', 'ios_packages_2015_08_12',
        'ios_packages_2015_08_04', 'ios_packages_2015_10_16','ios_packages_2015_10_21']
    sqldao = SqlDao()

    counter = defaultdict(set)
    for tbl in tbls:
        SQL = 'select app, path, hst, agent from %s where classified = 3' % tbl
        https = defaultdict(dict)
        for app, path, host, agent in sqldao.execute(SQL):
            https[app][path] = host+'$'+agent

        badapps = set()
        for app in https:
            cannotfind = True
            for path in https[app]:
                if path not in target:
                    cannotfind = False
            if cannotfind == True:
                badapps.add(app)
        for app in badapps:
            counter[app].add(tbl)

    for app in counter:
        print '[BAD APP]', app, counter[app]

def stat_classify_rst(tbl):
    target = {'/pep/gcc':'gsp1.apple.com',
              '/assets/com_apple_MobileAsset_Duet/com_apple_MobileAsset_Duet.xml':'mesu.apple.com',
              '/configurations/pep/pipeline/pipeline0.html':'configuration.apple.com',
              '/bag':'init-p01st.push.apple.com'}

    sqldao = SqlDao()
    SQL = 'select app, path, hst, agent from %s where classified = 3' % tbl
    https = defaultdict(dict)
    for app, path, host, agent in sqldao.execute(SQL):
        https[app][path] = host+'$'+agent

    badapps = set()
    for app in https:
        cannotfind = True
        for path in https[app]:
            if path not in target:
                cannotfind = False
            host = https[app][path]
        if cannotfind == True:
            badapps.add(app)

    for app in badapps:
        print app, https[app]
    print '=' * 10
    print 'Total', len(https), 'Can not find', len(badapps)



if __name__ == '__main__':
    if len(sys.argv) < 2:
        print 'error'
    elif sys.argv[1] == 'stat':
        stat_classify_rst(sys.argv[2])
    elif sys.argv[1] == 'all':
        stat_all()

