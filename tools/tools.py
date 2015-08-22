# -*- utf-8 -*-

import sys
from sqldao import SqlDao
from utils import loadfile
import math
from utils import load_pkgs
from package import Package
from nltk import FreqDist

def inst_cat(file_path):
  """
  Insert app's category into db
  """
  appNcats = {}
  import mysql.connector  
  cnx=mysql.connector.connect(user='root',password='123',host='127.0.0.1',database='fortinet')
  cursor = cnx.cursor()
  query = 'INSERT INTO android_app_details (package_name, app_title, category_code, offered_by, website) VALUES (%s, %s, %s, %s, %s)'
  def parser(ln):
    pkgname, name, category, company, website = ln.split('\t')
    try:
      cursor.execute(query, (pkgname, name, category, company, website))
    except:
      pass

  loadfile(file_path, parser)
  cnx.commit()
  cursor.close()
  cnx.close()

def transport_dns_info(mypath):
  """
  Insert pcap information into db
  """
  from os import listdir
  from os.path import isfile, join
  from scapy.all import rdpcap
  from scapy.layers import http
  from scapy.all import IP
  from scapy.all import DNSRR
  from scapy.all import DNS
  from sqldao import SqlDao
  startFlag = True
  sqldao = SqlDao()
  query = 'insert into host (app, host) values(%s, %s)'
  for f in listdir(mypath):
    if isfile(join(mypath,f)):
      try:
        file_path = join(mypath,f)
        appname = f[0:-5]
        caps = rdpcap(file_path)
        dnspackages=caps.filter(lambda(s): DNSRR in s)
        hosts = set()
        for dnspackage in dnspackages:
          amount = dnspackage[DNS].ancount
          for i in range(amount):
            hosts.add(dnspackage[4].qname[:-1])
        for host in hosts:
          sqldao.execute(query, (appname, host))
          print host
      except Exception as inst:
        print "Error", appname
        print inst
      
  sqldao.close()


def queryUrlToken():
  import urllib
  import mysql.connector
  from package import Package
  cnx=mysql.connector.connect(user='root',password='123',host='127.0.0.1',database='fortinet')
  cursor = cnx.cursor()
  query = "select app, path from packages where httptype = 0"
  cursor.execute(query)
  for app, path in cursor:
    package = Package()
    package.set_path(path)
    for k, v in package.querys.items():
      print "%s\t%s\t%s" % (app.encode('utf-8'),k.encode('utf-8'), v[0].encode('utf-8'))

def queryUrlToken(both):
  import urllib
  import mysql.connector
  from package import Package
  cnx=mysql.connector.connect(user='root',password='123',host='127.0.0.1',database='fortinet')
  cursor = cnx.cursor()
  query = "select app, path from packages where httptype = 0"
  cursor.execute(query)
  for app, path in cursor:
    package = Package()
    package.set_path(path)
    outln = app
    if both:
      for k, v in package.querys.items():
        print "%s\t%s\t%s" % (app.encode('utf-8'),k.encode('utf-8'), v[0].encode('utf-8'))
    else:
      for k,v in package.querys.items():
        outln = outln + '\t' + k
      if len(package.querys) > 0:
        print outln.encode('utf-8')

def select_uniq_hst():
  from nltk import FreqDist
  import mysql.connector
  counter = FreqDist()
  cnx=mysql.connector.connect(user='root',password='123',host='127.0.0.1',database='fortinet')
  cursor = cnx.cursor()
  query = 'select app, hst from packages where httptype = 0'
  cursor.execute(query)
  pairs = set()
  for app, host in cursor:
    if app + "$" + host not in pairs:
      pairs.add(app + "$" + host)
      counter.inc(host)
    
  for k,v in counter.items():
    print "%s\t%s" % (k,v)

def get_app_token():
  import mysql.connector
  cnx=mysql.connector.connect(user='root',password='123',host='127.0.0.1',database='fortinet')
  cursor = cnx.cursor()

def tf_idf(data_set = None):
  sqldao = SqlDao()
  if not data_set:
    data_set = []
    QUERY = 'SELECT path, app FROM packages'
    for path, app in sqldao.execute(QUERY):
      package = Package()
      package.set_path(path)
      package.set_app(app)

  pthsegApp = Relation()
  appCounter = set()
  for package in data_set:
    appCounter.add(package.app)
    for pathseg in package.path.split('/'):
      if len(pathseg) > 0:
        pthsegApp.add(pathseg, package.app)

  totalDoc = len(appCounter)
  INSERT = 'INSERT INTO tfidf (word, tfidf, app) VALUES (%s, %s, %s)'
  for pathseg, counter in pthsegApp.get().items():
    doc = len(counter)
    for app, count in counter.items():
      sqldao.execute(INSERT, (pathseg, math.log(1.0 * totalDoc / doc) * count, app))
  sqldao.close()

def update_ios():
  from app_info import AppInfos
  import consts
  apps = AppInfos
  sqldao = SqlDao()
  QUERY = 'UPDATE ios_packages_2015_05_04 SET app = %s, category = %s, company=%s, name = %s WHERE app = %s'
  print 'Start Updating'
  for app in apps.apps[consts.IOS].values():
    trackid = app.trackId
    package = app.package
    print package
    name = app.name
    company = app.company
    category = app.category
    sqldao.execute(QUERY, (package, category, company, name, trackid))

def gen_cmar_data(limit = None):
  
  records = load_pkgs(limit)
  train_data = []
  f_counter = FreqDist()
  f_company = Relation()

  for record in records:
    pathsegs = filter(None, record.path.split('/'))
    recordVec = []
    for pathseg in pathsegs:
      f_counter.inc(pathseg)
      f_company.add(pathseg, record.company)
  
  valid_f = set()
  for k, v in f_counter.items():
    if v > 1 and len(f_company.get()[k]) < 4:
      valid_f.add(k)

  app_index = {}
  feature_index = {}
  f_indx = 0

  for record in records:
    pathsegs = filter(None, record.path.split('/'))
    recordVec = []
    for pathseg in pathsegs:
      if pathseg not in valid_f:
        continue

      if pathseg not in feature_index:
        f_indx += 1
        feature_index[pathseg] = f_indx 
      
      recordVec.append(feature_index[pathseg])
    
    host = record.host
    if not host:
      host = record.dst

    train_data.append(((record.app, host), sorted(set(recordVec), reverse = True)))
  

  fwriter = open('record_vec.txt', 'w')
  # train_data
  # (app, [f1, f2, f3])
  # 
  recordHost = []
  encodedRecords = []
  for record in train_data:
    outstr = ''
    for f in record[1]:
      outstr = str(f) + ' ' + outstr
    if outstr:
      if record[0][0] not in app_index:
        f_indx += 1
        app_index[record[0][0]] = f_indx
      outstr += str(app_index[record[0][0]])
      recordHost.append(record[0][1])
      fwriter.write(outstr+'\n')

  fwriter.close()
  fwriter = open('app_index.txt', 'w')
  for k, v in app_index.items():
    fwriter.write(k+'\t'+str(v)+'\n')
  fwriter.close()
  fwriter = open('feature_index.txt', 'w')
  for k, v in feature_index.items():
    fwriter.write(k.encode('utf-8')+'\t'+str(v)+'\n')
  fwriter.close()
  fwriter = open('records_host.txt', 'w')
  for k in recordHost:
    fwriter.write(k.encode('utf-8')+'\n')
  fwriter.close()
  print 'output files are app_index.txt, feature_index.txt, record_vec.txt'
  print 'number of classes:', len(app_index)

def statCompany():
  from collections import defaultdict
  tbls = ['packages_20150210', 'packages_20150429', 'packages_20150509', 'packages_20150526']
  packages = []
  for tbl in tbls:
    packages += load_pkgs(DB = tbl)
  value_label = defaultdict(lambda : defaultdict(set))
  app_company = {}
  app_category = {}
  for pkg in packages:
    for k,v in pkg.queries.items():
      if pkg.secdomain == 'bluecorner.es' or pkg.host == 'bluecorner.es' or pkg.app == 'com.bluecorner.totalgym':
        #print 'OK contains bluecorner', pkg.secdomain
        pass
      map(lambda x : value_label[k][x].add(pkg.app), v)
      try:
        app_company[pkg.app] = pkg.company
        app_category[pkg.app] = pkg.category
      except:
        pass

  for key in value_label:
    for value in value_label[key]:
      output  = False
      for app in value_label[key][value]:
        if app in value:
          output = True
      if output and len(value_label[key][value]) > 1:
        print "%s; %s; %s; %s; %s" % (key, value, value_label[key][value], ';'.join(map(lambda app : app_company[app], value_label[key][value])), ','.join(map(lambda app : app_category[app], value_label[key][value])))

def test_suffix_tree():
  from utils import suffix_tree, loadExpApp
  def classify_suffix_app(app_suffix,value):
    value = value.split('.')
    node = app_suffix
    meet_first = False
    rst = []
    for i in reversed(value):
      if not meet_first and i in node.children:
        meet_first = True
      if meet_first:
        if i in node.children:
          rst.append(i)
          node = node.children[i]
        if len(node.children) == 0:
          return '.'.join(reversed(rst))
    return None
  app_name = '1.android.com.usmann.whitagramforandroid'
  exp_apps = loadExpApp()
  app_suffix = suffix_tree(exp_apps)
  label = classify_suffix_app(app_suffix, app_name)
  print label

def getExpAppList(folder):
  from os import listdir
  from os.path import isfile, join
  trackIds = dict()
  for date in listdir(folder):
    file_path = join(folder, date)
    print file_path
    trackIds[file_path] = set()
    for f in listdir(file_path):
      trackId = f[0:-5]
      trackIds[file_path].add(trackId)
  expIds = reduce(lambda x, y : x & y, trackIds.values())
  for expId in expIds:
    print expId


if __name__ == '__main__':
  if len(sys.argv) < 2:
    print 'error'
  elif sys.argv[1] == 'geturl':
    queryUrlToken(True)
  elif sys.argv[1] == 'trans':
    transportfiles('/Users/congzicun/Yunio/fortinet/apk_pcaps')
  elif sys.argv[1] == 'uniquehst':
    select_uniq_hst()
  elif sys.argv[1] == 'trans_dns':
    transport_dns_info('/Users/congzicun/Yunio/fortinet/apk_pcaps')
  elif sys.argv[1] == 'gen_record_vec':
    if len(sys.argv) == 3:
      gen_cmar_data(sys.argv[2])
    else:
      gen_cmar_data()
  elif sys.argv[1] == 'insertapp':
    inst_cat(sys.argv[2])
  elif sys.argv[1] == 'update':
    update_ios()
  elif sys.argv[1] == 'company':
    print 'stat'
    statCompany()
  elif sys.argv[1] == 'suffix':
    test_suffix_tree()
  elif sys.argv[1] == 'expids':
    getExpAppList(sys.argv[2])





####################################################
# def insert_rules(filepath):
#   import mysql.connector
#   from utils import loadfile
#   cnx=mysql.connector.connect(user='root',password='123',host='127.0.0.1',database='fortinet')
#   cursor = cnx.cursor()
#   query = 'insert into rules (app, hst, status) values (%s, %s, %s)'
#   def parser(ln):
#     hst, _, apps = ln.split('\t')
#     status, hst = hst.split(' ')
#     for app in apps.split(','):
#       cursor.execute(query, (app, hst, status))
#   loadfile(filepath, parser)
#   cnx.commit()  


####################################################
# def samplepcap(file_path):
#   from os import listdir
#   from os.path import isfile, join
#   import pyshark

#   startFlag = True
#   for f in listdir(file_path):
#     if isfile(join(file_path,f)):
#       cap =pyshark.FileCapture(join(file_path,f), keep_packets = False, display_filter='http')
#       try:
#         for p in cap:
#           print f
#           print p['http']
#           print '-'*10
#       except:
#         pass

####################################################
# def extractHttpHeads(file_path):
#   from os import listdir
#   from os.path import isfile, join
#   import pyshark
#   startFlag = True
#   for f in listdir(file_path):
#     if isfile(join(file_path,f)):
#       cap =pyshark.FileCapture(join(file_path,f), keep_packets = False, display_filter='http')
#       try:
#         for p in cap:
#           print f
#           print p['http']
#           print '-'*10
#       except:
#         pass

####################################################
#def transportfiles(mypath):
#   """
#   Insert pcap information into db
#   """
#   from os import listdir
#   from os.path import isfile, join
#   from utils import file2mysqlv2
#   startFlag = True
#   for f in listdir(mypath):
#     if isfile(join(mypath,f)):  
#       file2mysqlv2(join(mypath,f), f[0:-5])
