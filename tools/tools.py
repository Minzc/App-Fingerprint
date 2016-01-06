# -*- utf-8 -*-

import sys
from sqldao import SqlDao
from collections import defaultdict

def stat_classify_rst():
    target = {'/pep/gcc':'gsp1.apple.com', '/assets/com_apple_MobileAsset_Duet/com_apple_MobileAsset_Duet.xml':'mesu.apple.com'}
    sqldao = SqlDao()
    SQL = 'select app, path, host, agent from ios_packages_2015_08_10 where classified = 3'
    https = defaultdict(dict)
    for app, path, host, agent in sqldao.execute(SQL):
        https[app][path] = host

    badapps = set()
    for app in https:
        if len(https[app]) < 3:
            cannotfind = True
            for path in https[app]:
                if path  not in target:
                    cannotfind = False
            if cannotfind == True:
                badapps.add(app)

    for app in badapps:
        print https[app]
    print '=' * 10
    print 'Total', len(https), 'Can not find', len(badapps)



if __name__ == '__main__':
    if len(sys.argv) < 2:
        print 'error'
    elif 'stat':
        stat_classify_rst()

