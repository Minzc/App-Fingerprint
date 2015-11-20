#!/usr/bin/env python
# -*- coding: UTF-8 -*-
# 

import re
import string
import tldextract
from sqldao import SqlDao
from const.package import Package
import random
from const.app_info import AppInfos
import const.consts as consts
from collections import defaultdict

regex_enpunc = re.compile("[" + string.punctuation + "]")

extract = tldextract.TLDExtract(
    suffix_list_url="source/effective_tld_names.dat.txt",
    cache_file=False)

max_wordlen, min_wordlen = 100, 2


def rever_map(mapObj):
    """ revert the key value of a map """
    return {v: k for k, v in mapObj.items()}


def stratified_r_sample(N, records):
    strata = {}
    for record in records:
        if record.app not in strata:
            strata[record.app] = []
        strata[record.app].append(record)

    sample = []
    for app, pks in strata.items():
        n = max(1, 1.0 * len(pks) / len(records) * N)
        sample += reservoir_sample(n, pks)
    return sample


def reservoir_sample(N, records):
    sample = []

    for i, line in enumerate(records):
        if i < N:
            sample.append(line)
        elif i >= N and random.random() < N / float(i + 1):
            replace = random.randint(0, len(sample) - 1)
            sample[replace] = line
    return sample


def loadfile(filepath, parser):
    f = open(filepath)
    for ln in f:
        ln = ln.strip()
        if len(ln) != 0:
            parser(ln)
    f.close()


def name_clean(name):
    name = regex_enpunc.sub(' ', name)
    name = name.replace(u'®', ' ')
    return name


def url_clean(url):
    url = url.replace('http://', '').replace('www.', '').split('/')[0].split(':')[0]
    return url


def multi_replace(ln, chars, new):
    for char in chars:
        ln = ln.replace(char, new)
    return ln


def backward_maxmatch(s, wordDict, maxWordLen, minWordLen):
    postLst = []
    curL, curR = 0, len(s)
    while curR >= minWordLen:
        isMatched = False
        if curR - maxWordLen < 0:
            curL = 0
        else:
            curL = curR - maxWordLen
        while curR - curL >= minWordLen:  # try all subsets backwards
            if s[curL:curR] in wordDict:  # matched
                postLst.insert(0, (curL, curR))
                curR = curL
                if curR - maxWordLen < 0:
                    curL = 0
                else:
                    curL = curR - maxWordLen
                isMatched = True
                break
            else:  # not matched, try subset by moving left rightwards
                curL += 1

                # not matched, move the right end leftwards
        if not isMatched:
            curR -= 1

    wordLst = []
    for posS, postE in postLst:
        wordLst.append(s[posS:postE])
    return wordLst


def app_clean(appname):
    appsegs = appname.split('.')
    appname = ''
    for i in range(len(appsegs) - 1, -1, -1):
        appname = appname + appsegs[i] + '.'
    appname = appname[:-1]
    extracted = extract(appname)
    if extracted.suffix != '':
        appname = appname.replace('.' + extracted.suffix, '')
    return appname


def agent_clean(agent):
    agent = re.sub('[/]?[0-9][0-9.]*', ' ', agent)
    agent = re.sub('\\([^\\)][^\\)]*$', ' ', agent)
    agent = agent.replace(';', ' ').replace('(', ' ').replace(')', ' ').replace('/', ' ').replace('-', ' ').replace('_',
                                                                                                                    ' ')
    return agent


def top_domain(host):
    """
    Return the topdomain of given host
    """
    host = host.lower()
    host = host.split(':')[0]
    extracted = tldextract.extract(host)
    secdomain = None
    if len(extracted.domain) > 0:
        secdomain = "{}.{}".format(extracted.domain, extracted.suffix)
    return secdomain


def lower_all(strs):
    rst = []
    for astr in strs:
        if astr:
            rst.append(astr.lower())
        else:
            rst.append(astr)
    return rst


def process_path(path):
    import urlparse
    import urllib
    path = urllib.unquote(path).lower().replace(';', '?', 1).replace(';', '&')
    querys = urlparse.parse_qs(urlparse.urlparse(path).query, True)
    path = urlparse.urlparse(path).path
    return path, querys


def none2str(astr):
    if astr:
        return astr
    return ''


def add_appinfo(packages, app_type):
    appInfos = AppInfos
    for package in packages:
        appInfo = appInfos.get(app_type, package.app)
        if appInfo is None:
            print 'Error', package.app
        else:
            package.set_appinfo(appInfo)
    return packages


def load_data_set(trainTbls, appType):
    """
    Load data from given table
    Input
    :param trainTbls : a list of tables
    :param appType : IOS or ANDROID
    Output
    - record : {table_name : [list of packages]}
    """
    print 'Loading data set', trainTbls
    expApp = load_exp_app()

    def _keep_exp_app(package):
        return package.app in expApp[appType]

    records = {}
    for tbl in trainTbls:
        records[tbl] = load_pkgs(filterFunc=_keep_exp_app, DB=tbl, appType=appType)

    return records


def load_pkgs(DB, appType, limit, filterFunc=lambda x: True):
    records = []
    sqldao = SqlDao()
    if not limit:
        QUERY = consts.SQL_SELECT_HTTP_PKGS % DB
    else:
        QUERY = consts.SQL_SELECT_HTTP_PKGS_LIMIT % (DB, limit)
    print QUERY

    for pkgid, app, add_header, path, refer, host, agent, dst, method, raw in sqldao.execute(QUERY):
        if pkgid == 558126:
            print QUERY
            print app
            print path
            print host
            
        package = Package()
        package.set_app(app)
        package.set_path(path.decode('utf-8'))
        package.set_id(pkgid)
        package.set_add_header(add_header)
        package.set_refer(refer.decode('utf-8'))
        package.set_host(host)
        package.set_agent(agent.decode('utf-8'))
        package.set_dst(dst)
        package.set_content(raw)
        package.set_tbl(DB)
        package.set_method(method)

        if filterFunc(package):
            records.append(package)
    records = add_appinfo(records, appType)
    return records


def get_record_f(record):
    """Get package features"""
    features = filter(None, record.path.split('/'))

    for head_seg in filter(None, record.add_header.split('\n')):
        if len(head_seg) > 2:
            features.append(head_seg.replace(' ', '').strip())

    for agent_seg in filter(None, record.agent.split(' ')):
        if len(agent_seg) < 2:
            features.append(agent_seg.replace(' ', ''))
    host = record.host if record.host else record.dst
    features.append(host)

    return features


def load_exp_app():
    expApp = {consts.IOS: set(), consts.ANDROID: set()}
    appInfos = AppInfos
    for line in open("resource/exp_app.txt"):
        app_type, app = line.lower().strip().split(':')
        if app_type == consts.IOS_STR:
            app_type = consts.IOS
        elif app_type == consts.ANDROID_STR:
            app_type = consts.ANDROID
        package = appInfos.get(app_type, app.strip().lower()).package
        expApp[app_type].add(package)
    return expApp


def suffix_tree(apps):
    """
    Build app pkg names' suffix tree
    """

    class node:
        def __init__(self, value):
            self.parents = {}
            self.value = value
            self.children = {}

    root = node(None)
    for app in apps:
        nd = root
        for seg in reversed(app.split('.')):
            if seg not in nd.children:
                new_node = node(seg)
                nd.children[seg] = new_node
            nd = nd.children[seg]
    return root


def get_top_domain(host):
    import tldextract
    host = host.lower()
    host = host.split(':')[0].replace('www.', '').replace('http://', '')
    extracted = tldextract.extract(host)

    if len(extracted.domain) > 0:
        return "{}.{}".format(extracted.domain, extracted.suffix)
    return None


def longest_common_substring(s1, s2):
    m = [[0] * (1 + len(s2)) for i in xrange(1 + len(s1))]
    longest, x_longest = 0, 0
    for x in xrange(1, 1 + len(s1)):
        for y in xrange(1, 1 + len(s2)):
            if s1[x - 1] == s2[y - 1]:
                m[x][y] = m[x - 1][y - 1] + 1
                if m[x][y] > longest:
                    longest = m[x][y]
                    x_longest = x
            else:
                m[x][y] = 0
    return s1[x_longest - longest: x_longest]


def load_xml_features():
    """
    Output
    - appFeatures : {appInfo : set()}
    """
    from os import listdir
    from os.path import isfile, join
    from const.app_info import AppInfos
    folder = './resource/Infoplist/'
    appFeatures = defaultdict(set)
    for f in listdir(folder):
        filePath = join(folder, f)
        if isfile(filePath):
            trackId = f[0:-4]
            app = AppInfos.get(consts.IOS, trackId).package
            features = _parse_xml2(filePath)
            features.add((u'PACKAGE_NAME', app))
            features.add((u'TRACK_ID', trackId))
            appFeatures[app] = features
    return appFeatures


def _parse_xml2(filePath):
    import plistlib
    plistObj = plistlib.readPlist(filePath)

    def _flat(plistObj, parent):
        values = set()
        if type(plistObj) == plistlib._InternalDict:
            for key, value in plistObj.items():
                if type(key) != unicode:
                    key = key.decode('ascii')

                if type(value) == list:
                    values |= _flat(value, parent + '_' + key)
                elif type(value) == plistlib._InternalDict:
                    values |= _flat(value, parent + '_' + key)
                elif type(value) == str:
                    value = value.decode('ascii').lower()
                    values.add((parent + '_' + key, value))
                elif type(value) == unicode:
                    value = value
                    value = value.lower()
                    values.add((parent + '_' + key, value))
                else:
                    pass
        elif type(plistObj) == list:
            for value in plistObj:
                if type(value) == list:
                    values |= _flat(value, parent)
                elif type(value) == plistlib._InternalDict:
                    values |= _flat(value, parent)
                elif type(value) == str:
                    value = value.lower()
                    values.add((parent, value))
                elif type(value) == unicode:
                    value = value
                    value = value.lower()
                    values.add((parent, value))
                else:
                    pass
        return values
    featureDict = _flat(plistObj, '')
    return featureDict


def if_version(v):
    """
    Check if a string is version type (x1.23123)
    """
    v = re.sub('[0-9]\.[.0-9]+', '', v)
    return len(v) <= 2


def flatten(d):
    import collections
    '''
    Flat nested dictionary to lists
    '''
    items = []
    for k, v in d.items():
        if isinstance(v, collections.MutableMapping):
            children = flatten(v)
            map(lambda child: child.insert(0, k), children)
            items.extend(children)
        else:
            items.append([k, v])
    return items


def unescape(s):
    s = s.replace("&lt;", "<")
    s = s.replace("&gt;", ">")
    # this has to be last:
    s = s.replace("&amp;", "&")
    return s


def load_info_features(_parse_xml):
    from os import listdir
    from os.path import isfile, join
    folder = './resource/Infoplist/'
    appFeatures = defaultdict(set)
    for f in listdir(folder):
        filePath = join(folder, f)
        if isfile(filePath):
            trackId = f[0:-4]
            app = AppInfos.get(consts.IOS, trackId).package
            features = _parse_xml(filePath)
            features['BUNDLE_ID'] = app
            appFeatures[app] = features
    return appFeatures


# def _parse_xml(filePath):
#     import plistlib
#     plistObj = plistlib.readPlist(filePath)
#     features = set()
#     for key in VALID_FEATURES:
#         if key in plistObj:
#             value = unescape(plistObj[key].lower())
#             features.add(value)
#     return features
