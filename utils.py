#!/usr/bin/env python
# -*- coding: UTF-8 -*-
# 
  
import re
import string
import tldextract
from sqldao import SqlDao
from package import Package
import random
from app_info import AppInfos
import consts

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
 
  for i,line in enumerate(records):
    if i < N:
      sample.append(line)
    elif i >= N and random.random() < N/float(i+1):
      replace = random.randint(0,len(sample)-1)
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
  url = url.replace('http://', '').replace('www.','').replace('-', '.').split('/')[0].split(':')[0]
  return url

def multi_replace(ln, chars, new):
  for char in chars:
    ln = ln.replace(char, new)
  return ln


def backward_maxmatch( s, dict, maxWordLen, minWordLen ):
  postLst = []
  curL, curR = 0, len(s)
  while curR >= minWordLen:
    isMatched = False
    if curR - maxWordLen < 0:
      curL = 0
    else:
      curL = curR - maxWordLen
    while curR - curL >= minWordLen: # try all subsets backwards
      if s[curL:curR] in dict: # matched
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
  for i in range(len(appsegs)-1,-1,-1):
    appname = appname + appsegs[i] + '.'
  appname = appname[:-1]
  extracted = extract(appname)
  if extracted.suffix != '':
    appname = appname.replace('.'+extracted.suffix, '')
  return appname

def agent_clean(agent):
  agent = re.sub('[/]?[0-9][0-9.]*', ' ', agent)
  agent = re.sub('\\([^\\)][^\\)]*$', ' ', agent) 
  agent = agent.replace(';',' ').replace('(',' ').replace(')',' ').replace('/',' ').replace('-',' ').replace('_',' ')
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

def processPath(path):
  import urlparse
  import urllib
  path = urllib.unquote(path).lower().replace(';','?',1).replace(';','&')
  querys = urlparse.parse_qs(urlparse.urlparse(path).query, True)
  path = urlparse.urlparse(path).path
  return path, querys

def none2str(astr):
  if astr:
    return astr
  return ''

def add_appinfo(packages):
  appInfos = AppInfos()
  for package in packages:
    appInfo = appInfos.get(consts.IOS, package.app)
    if appInfo == None:
      print 'Error', package.app
    package.set_appinfo(appInfo)
  return packages

def load_pkgs(limit = None, filterFunc=lambda x : True, DB="packages"):
    records = []
    sqldao = SqlDao()
    QUERY = None
    if not limit:
        QUERY = "select id, app, add_header, path, refer, hst, agent, dst, raw from %s where method=\'GET\'" % DB
    else:
        QUERY = "select id, app, add_header, path, refer, hst, agent, dst, raw from %s where method=\'GET\' limit %s" % (DB, limit)
    print QUERY

    for id, app, add_header, path, refer, host, agent, dst, raw in sqldao.execute(QUERY):
        package = Package()
        package.set_app(app)
        package.set_path(path)
        package.set_id(id)
        package.set_add_header(add_header)
        package.set_refer(refer)
        package.set_host(host)
        package.set_agent(agent)
        package.set_dst(dst)
        package.set_content(raw)
       
        if filterFunc(package):
            records.append(package)
    records = add_appinfo(records)
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

def loadExpApp():
    expApp=set()
    for app in open("resource/exp_app.txt"):
        expApp.add(app.strip().lower())
    return expApp

def load_appinfo():
  QUERY = 'SELECT app, name, company, category FROM apps'
  app_company = {}
  app_name = {}
  sqldao = SqlDao()
  for app, name, company, category in sqldao.execute(QUERY):
      app_company[app.lower()] = company
      app_name[app.lower()] = name
  return app_company, app_name

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
  host = host.split(':')[0].replace('www.', '').replace('http://','')
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

