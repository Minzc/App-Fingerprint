# -*- coding:utf-8 -*-
import sys

from nltk import FreqDist

from utils import loadfile, Relation, top_domain, lower_all, app_clean, load_pkgs, loadExpApp
from sqldao import SqlDao
from package import Package
from collections import defaultdict, namedtuple


def stat_hstNapp(filepath):
    counter = FreqDist()

    def parser(ln):
        if len(ln.split('\t')) < 2:
            return
        app, host, time = ln.split('\t')
        # remove port
        colon_pos = host.find(':')
        if colon_pos != -1:
            host = host[0:colon_pos]
        host_segs = host.split('.')
        # only keep the top domain
        if len(host_segs) >= 2:
            host = host_segs[-2] + '.' + host_segs[-1]
            counter.inc(host, int(time))

    loadfile(filepath, parser)
    for k, v in counter.items():
        print "%s\t%s" % (k, v)


def stat_catNapp(filepath):
    categories = loadcategory()
    chart = {}

    def parser(ln):
        if len(ln.split('\t')) < 2:
            return
        cat, host, time = ln.split('\t')
        # only keep the top domain

        host = tpdomain(host)
        if host not in chart:
            chart[host] = [0] * len(categories)
        chart[host][categories[cat]] += 1

    loadfile(filepath, parser)
    for k, v in chart.items():
        sys.stdout.write(k)
        counter = 0
        for i in range(len(categories)):
            if (v[i] != 0):
                counter += 1
            sys.stdout.write('\t' + str(v[i]))
        sys.stdout.write('\t' + str(counter))
        print


def statUrlToken(filepath):
    import urllib

    """
    find out urls contain com
    """

    def parser(ln):
        ln = ln.lower()
        if len(ln.split(' ')) == 1:
            return
        app, path = ln.split(' ')
        path = urllib.unquote(path)
        path_segs = path.split('&')

        for path_seg in path_segs:
            if 'com.' in path_seg and '=' in path_seg:
                parameter = path_seg.split('=')[0]
                print "%s\t%s\t%s" % (parameter, path_seg, app)

    loadfile(filepath, parser)




from nltk import FreqDist



def tofile(writer, row_counter, row_indx):
    ln = row_indx.strip().replace(',', ' ')
    for app in apps:
        if app in row_counter:
            ln = ln + ',' + str(row_counter[app])
        else:
            ln = ln + ',0'
    writer.write(ln + '\n')




def host_freq_mine(filepath):
    """
    segment host and count number of parts
    """
    from nltk import FreqDist
    from utils import multi_replace

    counter = FreqDist()

    def parser(ln):
        ln = multi_replace(ln, ['.', '_', '-'], '')
        lnsegs = ln.split(' ')
        for lnseg in lnsegs:
            counter.inc(lnseg)

    for k, v in counter.items():
        print "%s\t%s" % (k, v)


def hst_clst_id(filepath):
    """
    get hosts that occurr in only one cluster
    """
    filecontent = []

    def parser(ln):
        filecontent.append(ln)

    hst_app = {}
    indx = 0
    loadfile(filepath, parser)
    for ln in filecontent:
        hosts, apps = ln.split('\t')
        for host in hosts.split(','):
            if host not in hst_app:
                hst_app[host] = set()
            hst_app[host].add(indx)
        indx += 1
    candidate_host = {}
    for k, v in hst_app.items():
        if len(v) == 1:
            n = v.pop()
            if n not in candidate_host:
                candidate_host[n] = set()
            candidate_host[n].add(k)

    for k, v in candidate_host.items():
        if len(v) == 1:
            print "%s\t$$$$$$%s" % ('\t'.join(v), filecontent[k])


def hst_n_secdomain():
    """
    hst secdomain:app
    """
    import mysql.connector
    from package import Package

    cnx = mysql.connector.connect(user='root', password='123', host='127.0.0.1', database='fortinet')
    cursor = cnx.cursor()
    query = 'select app, hst from packages'
    cursor.execute(query)
    hst = {}
    for app, host in cursor:
        package = Package()
        package.set_host(host)
        host = package.host
        secdomain = package.secdomain

        if secdomain != None and len(host) > len(secdomain):
            if secdomain not in hst:
                hst[secdomain] = set()
            hst[secdomain].add(app + ':' + host)
    for k, v in hst.items():
        print "%s\t%s" % (k, '\t'.join(v))
        print


def adserverNkey():
    """
    ad service ,app, key
    """
    import mysql.connector
    from package import Package
    from nltk import FreqDist

    cnx = mysql.connector.connect(user='root', password='123', host='127.0.0.1', database='fortinet')
    cursor = cnx.cursor()
    query = 'select app, path, hst from packages'

    apphst = FreqDist()
    apphstkeys = FreqDist()
    cursor.execute(query)
    for app, path, hst in cursor:
        package = Package()
        package.set_host(hst)
        package.set_path(path)
        for k, v in package.queries.items():
            id = app + '##' + hst
            id2 = id + '##' + v[0]
            if len(v[0]) > 1:
                apphst.inc(id)
                apphstkeys.inc(id2)

    for k, v in apphstkeys.items():

        app, hst, token = k.split('##')
        if v == apphst[app + '##' + hst]:
            print k




def group_path():
    from package import Package
    from utils import app_clean

    QUERY = 'SELECT app, name, path,hst FROM packages'
    sqldao = SqlDao()
    sqldao2 = SqlDao()
    cursor = sqldao.execute(QUERY)
    QUERY = 'INSERT INTO rules (app, tpdomain, hst, company, agent) VALUES (%s,%s,%s,%s,%s)'
    for app, name, path, hst in cursor:
        package = Package()
        app = app_clean(app)
        package.set_app(app)
        package.set_name(name)
        package.set_path(path)
        pathsegs = package.path.split('/')
        evidence = ''
        store = False
        for p in pathsegs:
            for nameseg in package.name.split(' '):
                if nameseg in p and len(nameseg) > 0:
                    store = True
                    evidence = evidence + '$' + nameseg
            for appseg in package.app.split('.'):
                if appseg in p and len(nameseg) > 0:
                    store = True
                    evidence = evidence + '$' + appseg
        if store:
            # print 'insert', (app, evidence, path, hst,'')
            sqldao2.execute(QUERY, (app, evidence, path, hst, ''))


def group_host():
    sqldao = SqlDao()
    QUERY = 'SELECT app, feature FROM appfeatures'
    cursor = sqldao.execute(QUERY)
    appfs = Relation()
    for app, feature in cursor:
        appfs.add(app, feature)

    QUERY = 'SELECT app,add_header,hst from packages group by app, hst, add_header'
    relation = Relation()  # host -> company
    companyfs = Relation()

    hosturl = Relation()
    has_addhder = set()

    for app, header, host in cursor:
        app = app.lower()
        header = header.lower()

        app = app_clean(app)
        company = app.split('.')[-1]


        # add product
        for p in app.split('.')[:-1]:
            product.add(company, p)

        # add name
        for p in name.split(' '):
            if len(p) > 1:
                productname.add(company, p)

        sechost = top_domain(host)
        if sechost != None:
            relation.add(sechost, company)
            hosturl.add(sechost, host)
            if 'x-requested-with' in header:
                has_addhder.add(sechost)

    INSERT = 'INSERT INTO rules (app, tpdomain, hst, company, agent) VALUES (%s,%s,%s,%s,%s)'
    for host, companies in relation.get().items():
        if len(companies) == 1:
            url = host
            if len(hosturl.get()[host]) == 1:
                for p in hosturl.get()[host]:
                    url = p

            for company in companies:
                if company in host:
                    sqldao.execute(INSERT, ('app', '', host, company, 1))
                elif host in has_addhder:
                    sqldao.execute(INSERT, ('app', '', host, company, 2))
                elif host.split('.')[0] in company or host.split('-')[0] in company:
                    sqldao.execute(INSERT, ('app', host.split('.')[0], host, company, 3))
                else:
                    value = 0
                    # product name in host
                    evidence = ''
                    for p in product.get().get(company, ()):
                        if host == 'schoolofdragons.com':
                            print '$$$$', p
                        if p in host.split('.')[0]:
                            evidence = p
                            value = 4
                    if value == 0:
                        for p in productname.get().get(company, ()):
                            if p in host.split('.')[0]:
                                evidence += p
                                value = 5
                    if value == 0:
                        for p in productname.get().get(company, ()):
                            if p in url.split('.')[:-1]:
                                evidence += p
                                value = 6
                                host = url
                    if value == 0:
                        for p in product.get().get(company, ()):
                            if p in url.split('.')[:-1]:
                                evidence = p
                                value = 7
                                host = url

                    sqldao.execute(INSERT, ('app', evidence, host, company, value))
    sqldao.close()


from utils import name_clean
from utils import none2str


def app_features():
    def gen_features(segs):
        for i in range(len(segs)):
            if len(segs[i]) > 0:
                for j in range(len(segs)):
                    if len(segs[j]) > 0:
                        msg = ''
                        if i == j:
                            msg = segs[i]
                        else:
                            msg = segs[i] + '.' + segs[j]
                            fapp.add(msg, app)
                            fcategory.add(msg, category)
                            fcompany.add(msg, company)
                            msg = segs[i] + segs[j]
                        fapp.add(msg, app)
                        fcategory.add(msg, category)
                        fcompany.add(msg, company)

    QUERY = 'SELECT app, name, company, category, dev FROM apps'
    sqldao = SqlDao()
    fapp = Relation()
    fcompany = Relation()
    fcategory = Relation()

    for app, name, company, category, dev in sqldao.execute(QUERY):
        app, name, company, category, dev = lower_all((app, name, company, category, dev))

        appsegs = app_clean(app).split('.')
        namesegs = name_clean(name).split(' ')
        companysegs = name_clean(none2str(company)).split(' ')
        devsegs = name_clean(none2str(dev)).split(' ')

        gen_features(appsegs)
        gen_features(namesegs)
        gen_features(companysegs)
        gen_features(devsegs)

    def insert(query, relation):
        for f, objs in relation.get().items():
            if f == 'sony':
                print app
            if len(objs) == 1:
                for obj in objs:
                    sqldao.execute(QUERY, (obj, f))

    QUERY = 'INSERT INTO appfeatures (app, feature) VALUES (%s,%s)'
    insert(QUERY, fapp)

    QUERY = 'INSERT INTO appfeatures (company, feature) VALUES (%s,%s)'
    insert(QUERY, fcompany)

    QUERY = 'INSERT INTO appfeatures (category, feature) VALUES (%s,%s)'
    insert(QUERY, fcategory)

    sqldao.close()


def gen_rules(time):
  from utils import load_dict


  dic = load_dict()
  sqldao = SqlDao()
  QUERY = 'SELECT app, feature, company, category FROM appfeatures'
  cursor = sqldao.execute(QUERY)
  fapp = {}
  fcompany = {}
  fcategory = {}
  for app, feature, company, category in cursor:
      if app:
          fapp[feature] = app
      if company:
          fcompany[feature] = company
      if category:
          fcategory[feature] = category

  QUERY = 'SELECT app,hst,company,add_header from packages group by app, hst'
  INSERT = 'INSERT INTO rules (app, hst) VALUES (%s,%s)'

  rulesdao = SqlDao()
  hostapp = Relation()
  hostcompany = Relation()

  for app, hst, company, header in sqldao.execute(QUERY):
      app, hst, company, header = lower_all((app, hst, company, header))
      hst = hst.split(':')[0].replace('www.', '')

      # if time == 2 and 'x-requested-with' in header:
      # 	continue
      if company:
          hostcompany.add(hst, company)
      # hst = hst.replace('-','')
      hostapp.add(hst, app)

  for hst, apps in hostapp.get().items():
      if hst == 'tzooimg.com':
          print apps
      if len(apps) == 1 and hst[-1] != '.':
          for app in apps:
              rulesdao.execute(INSERT, (app, hst))

  # INSERT = 'INSERT INTO rules (company, hst) VALUES (%s,%s)'
  # for hst, companies in hostcompany.get().items():
  # 	if len(companies) == 1:
  # 		company = companies.pop()
  # 		prdct = set()
  # 		wds = backward_maxmatch(hst, dic, max_wordlen, min_wordlen)
  # 		for wd in wds:
  # 			if hst == 'whalesharkmedia.d1.sc.omtrdc.net':
  # 				print prdct,wd
  # 				print fcompany.get(wd,'')
  # 			if fcompany.get(wd,'') == company:
  # 				prdct.add(company)
  # 		if len(prdct) == 1:
  # 			rulesdao.execute(INSERT, (prdct.pop(), hst))

  rulesdao.close()


def stat_origin_header():
    QUERY = "select id, app, add_header, path, refer, hst, agent, company,name from packages where httptype=0"
    records = []
    sqldao = SqlDao()

    for id, app, add_header, path, refer, host, agent, company, name in sqldao.execute(QUERY):
        package = Package()
        package.set_app(app)
        package.set_path(path)
        package.set_id(id)
        package.set_add_header(add_header)
        package.set_refer(refer)
        package.set_host(host)
        package.set_agent(agent)
        package.set_company(company)
        package.set_name(name)
        records.append(package)
    relation = Relation()
    for record in records:
        for h in record.add_header.split('\n'):
            if 'origin' in h:
                relation.add(h, record.app)
    for k, v in relation.get().items():
        if len(v) == 1:
            print k, v
        else:
            print '#', k, v


def stat_refer():
    QUERY = "select id, app, add_header, path, refer, hst, agent, company,name from packages where httptype=0"
    records = []
    sqldao = SqlDao()

    for id, app, add_header, path, refer, host, agent, company, name in sqldao.execute(QUERY):
        if refer:
            package = Package()
            package.set_app(app)
            package.set_path(refer)
            package.set_id(id)
            package.set_add_header(add_header)
            package.set_refer(refer)
            package.set_host(host)
            package.set_agent(agent)
            package.set_company(company)
            package.set_name(name)
            records.append(package)

    parameters = set()
    for record in records:
        if record.app in record.origPath:
            for k in record.queries:
                if record.app in record.queries[k]:
                    parameters.add(k)
    apps = set()
    pkgNum = 0
    for record in records:
        for parameter in parameters:
            if parameter in record.queries:
                print '1', record.queries[parameter]
                if record.app not in record.queries[parameter]:
                    print '2', record.app, record.queries[parameter], parameter, record.origPath
                else:
                    pkgNum += 1
                    apps.add(record.app)

    print 'pkgNum:', pkgNum, 'appNum:', len(apps), 'Total:', len(records)


# def stat_path():
# 	records = load_pkgs()

# 	specialPath = set()
# 	apps = set()
# 	pkgNum = 0
# 	for record in records:
# 		if record.app in record.path:
# 			pkgNum += 1
# 			apps.add(record.app)
# 			specialPath.add(record.path)


# 	print 'pkgNum:', pkgNum, 'appNum:', len(apps), 'Total:', len(records)

def stat_host_app():
    from utils import none2str

    records = load_pkgs()
    relation = Relation()
    appCompany = {}
    appName = {}
    for record in records:
        relation.add(record.host, record.app + '$' + none2str(record.company) + '$' + none2str(record.name))
        appCompany[record.app] = record.company
    apps = set()
    for k, v in relation.get().items():
        if len(v) == 1:
            a = v.items()[0][0]
            apps.add(a)
            print k, v
    print 'apps:', len(apps)

def output_kv():
    sqldao = SqlDao()
    for path, host, app in sqldao.execute('SELECT path, hst, app FROM packages WHERE classified is NULL and httptype=0'):
      package = Package()
      package.set_path(path)
      print '-' * 20
      print app, host
      for k, v in package.queries.items():
        print k, v

def test_client_slot():
  records = load_pkgs()
  relations = defaultdict(set)
  for record in records:
    if 'client' in record.queries and 'slotname' in record.queries:
      client = record.queries['client'][0]
      slotname = record.queries['slotname'][0]
      relations[client + '$' + slotname].add(record.app)

  print len(relations)
  for k, v in relations.iteritems():
    if len(v) > 1:
      print k, v

def test_sdk_id():
  records = load_pkgs()
  relations = defaultdict(set)
  for record in records:
    if 'id' in record.queries and 'ads.mopub.com' == record.host:
      appid = record.queries['id'][0]
      relations[appid].add(record.app)

  print len(relations)
  for k, v in relations.iteritems():
    if len(v) > 1:
      print k, v

def stat_post_content():
  QUERY = 'select id, app, add_header, path, refer, hst, agent, company,name, dst, raw from packages where httptype=0 and method=\'POST\''
  records = []
  sqldao = SqlDao()
  for id, app, add_header, path, refer, host, agent, company,name, dst,raw  in sqldao.execute(QUERY):
    package = Package()
    package.set_app(app)
    package.set_path(path)
    package.set_id(id)
    package.set_add_header(add_header)
    package.set_refer(refer)
    package.set_host(host)
    package.set_agent(agent)
    package.set_company(company)
    package.set_name(name)
    package.set_dst(dst)
    package.set_content(raw)
    records.append(package)
  count = 0
  for r in records:
    s = r.content.split('\n')
    if r.app in r.content:
      count += 1
  print "contain app", count, "total", len(records)

def query_length():
  QUERY = 'SELECT count(*) FROM packages WHERE app = \'%s\' and method = \'GET\''
  sqldao = SqlDao()
  rst = defaultdict(list)
  for ln in open('tmp'):
    ln = ln.strip()
    for i in sqldao.execute(QUERY % ln):
      rst[ln].append(i[0])
  QUERY = 'SELECT count(*) FROM packages_2000 WHERE app = \'%s\' and method = \'GET\''
  for ln in open('tmp'):
    ln = ln.strip()
    for i in sqldao.execute(QUERY % ln):
      rst[ln].append(i[0])
  for k,v in rst.items():
      print v[0],',' ,v[1]

def findExpApps():
    QUERY = "SELECT DISTINCT(app) FROM %s WHERE method = \'GET\'"
    tbls = ["packages_20150210", "packages_20150429", "packages_20150509", "packages_20150526"]
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

def rmOtherApp(tbls=["packages_20150210", "packages_20150429", "packages_20150509", "packages_20150526"]):
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

def batchTest(outputfile):
  #tbls = ["packages_20150210", "packages_20150616", "packages_20150509", "packages_20150526"]
  expApp = loadExpApp()
  tbls = ["packages_20150210",  "packages_20150509", "packages_20150526"]
  featureTbl = defaultdict(lambda : defaultdict( lambda : defaultdict( lambda : defaultdict(set))))
  valueAppCounter = defaultdict(set)
  valueCompanyCounter = defaultdict(set)
  totalPkgs = {}
  appDict = set()
  ##################
  # Load Data
  ##################
  for tbl in tbls:
    pkgs = load_pkgs(None,  DB = tbl)    
    totalPkgs[tbl] = pkgs
    for pkg in pkgs:
      appDict.add(pkg.app)
      for k,v in pkg.queries.items():
        map(lambda x : featureTbl[pkg.secdomain][pkg.app][k][x].add(tbl), v)
        map(lambda x : valueAppCounter[x].add(pkg.app), v)
  
  ##################
  # Count
  ##################
  # secdomain -> key -> (app, score)
  keyScore = defaultdict(lambda : defaultdict(lambda : {'app':set(), 'score':0}))
  violate = defaultdict(lambda : defaultdict(set))
  covered = defaultdict(lambda : defaultdict(set))
  for secdomain in featureTbl:
    for app in featureTbl[secdomain]:
      for k in featureTbl[secdomain][app]:
        for v in featureTbl[secdomain][app][k]:
          covered[secdomain][k].add(app)
          if len(featureTbl[secdomain][app][k]) > 1:
            violate[secdomain][k].add(app)
          if len(valueAppCounter[v]) == 1:
            cleaned_k = k.replace("\t", "")
            keyScore[secdomain][cleaned_k]['score'] += (len(featureTbl[secdomain][app][k][v]) - 1) / float(len(featureTbl[secdomain][app][k]))
            keyScore[secdomain][cleaned_k]['app'].add(app)
            # try:
            #   fw.write("%s %s %s %s\n" % (secdomain, app, k, v.replace('\n','').replace(' ', ''), len(featureTbl[secdomain][app][k])))
            # except:
            #   pass
  #fw = open(outputfile+".score", 'w')
  Rule = namedtuple('Rule', 'secdomain,key,score,appNum')
  general_rules = defaultdict(list)
  for secdomain in keyScore:
    for key in keyScore[secdomain]:
      if secdomain == 'facebook.com' and key == 'app_id':
        print 'Key=', key
        print "keyScore[secdomain][key]['app']=", keyScore[secdomain][key]['app']
        print "keyScore[secdomain][key]['score']=", keyScore[secdomain][key]['score']

      if len(keyScore[secdomain][key]['app']) == 1 or (keyScore[secdomain][key]['score'] == 0 and 'id' not in key.lower()):
        continue
      general_rules[secdomain].append(Rule(secdomain, key, keyScore[secdomain][key]['score'], len(keyScore[secdomain][key]['app'])))
      # try:
      #   fw.write("%s\t%s\t%s\t%s\n" % (secdomain, key, keyScore[secdomain][key]['score'], len(keyScore[secdomain][key]['app'])))
      # except:
      #   pass
  # fw.close()
  for secdomain in general_rules:
    general_rules[secdomain] = sorted(general_rules[secdomain], key=lambda rule: rule.score, reverse = True)

  specific_rules = defaultdict(lambda : defaultdict( lambda : defaultdict( lambda : defaultdict(lambda : {'score':0, 'count':0}))))
  ruleCover = defaultdict(int)
  covered_app = set()
##############
  debug_counter = 0
##############
  for tbl in totalPkgs:
    for pkg in totalPkgs[tbl]:
      if pkg.secdomain in general_rules:
        for rule in general_rules[pkg.secdomain]:
          if rule.key in pkg.queries:
            ruleCover[rule] += 1
            covered_app.add(pkg.app)
            for value in pkg.queries[rule.key]:
              if len(valueAppCounter[value]) == 1:
                # todo DEBUG
                pkg.secdomain = ''
                specific_rules[pkg.secdomain][rule.key][value][pkg.app]['score'] = rule.score
                specific_rules[pkg.secdomain][rule.key][value][pkg.app]['count'] += 1
                debug_counter += 1
  
  print "specific_rules", len(specific_rules), debug_counter

  # fw = open(outputfile+'.rule_cover', 'w')
  # for rule in ruleCover:
  #   try:
  #     fw.write("%s\t%s\t%s\t%s\t%s\n" % ( rule.secdomain, rule.key, rule.score, rule.app, ruleCover[rule]))
  #   except:
  #     pass
  # fw.write("Covered App:" + str(len(covered_app)) + "\n")
  # fw.close()
  
  # fw = open(outputfile+'.total_rules', 'w')
  # for secdomain in specific_rules:
  #   for key in specific_rules[secdomain]:
  #     for app in specific_rules[secdomain][key]:
  #       for value in specific_rules[secdomain][key][app]:
  #         try:
  #           fw.write("%s\t%s\t%s\t%s\t%s\n" % ( secdomain, key, value, app, specific_rules[secdomain][key][app]))
  #         except:
  #           pass
  # fw.close()


  ###################
  # Test
  ###################
  pkgs = load_pkgs(DB = "packages_20150429")
  predict_rst = {}
  debug = defaultdict(lambda : defaultdict(lambda : defaultdict(int)))
  total = 0
  for pkg in pkgs:
    max_score = -1
    occur_count = -1
    predict_app = None
    backup_rst = None
    token, value, secdomain = None, None, None
    if len(pkg.queries) > 0:
      total += 1
    # todo DEBUG
    pkg.secdomain = ''
    if pkg.secdomain in specific_rules:
      for k in specific_rules[pkg.secdomain]:
        if k in pkg.queries:
          for v in pkg.queries[k]:
            if v in appDict:
                backup_rst = v
            if v in specific_rules[pkg.secdomain][k]:
              for app, score_count in specific_rules[pkg.secdomain][k][v].iteritems():
                score,count = score_count['score'], score_count['count']
                update = False
                if score > max_score:
                  predict_app = app
                  max_score = score
                  occur_count = count
                  update = True
                elif score == max_score and count > occur_count:
                  predict_app = app
                  occur_count = count
                  update = True
                if update:
                  token = k
                  value = v
                  secdomain = pkg.secdomain
    
    predict_app = backup_rst if not predict_app else predict_app

    predict_rst[pkg.id] = (predict_app, pkg.app)
    if predict_app != pkg.app:
      debug[secdomain][token][value] += 1

  #################
  # Evaluate
  #################
  covered_app = set()
  precision = 0
  recall = 0
  for value in predict_rst.values():
    if value[0] != None:
      recall += 1
      if value[0] == value[1]:
        precision += 1
        covered_app.add(value[1])
      else:
        print value[0], value[1]
  print "Precision: %s (%s / %s) Recall: %s (%s / %s) App: %s " % (float(precision)/recall, precision, recall, float(recall) / total, recall,total,len(covered_app))
  # fw = open(outputfile+'.debug', 'w')
  # for secdomain in debug:
  #   for token in debug[secdomain]:
  #     for value in debug[secdomain][token]:
  #       fw.write("%s\t%s\t%s\t%s\n" % ( secdomain, token, value, debug[secdomain][token][value] ))
  # fw.close()


def statUrlPcap(outputfile):
    import urlparse
    import urllib
    import tldextract

    appUrl = defaultdict(set)
    sqldao = SqlDao()
    for app, url, fileName in sqldao.execute('SELECT * FROM url_apk'):
        # path = urllib.unquote(path).lower().replace(';', '?', 1).replace(';', '&')
        # origPath = path
        # querys = urlparse.parse_qs(urlparse.urlparse(path).query, True)
        # path = urlparse.urlparse(path).path
        # appUrl[url].add(app)
        extracted = tldextract.extract(url)
        secdomain = None
        if len(extracted.domain) > 0:
            secdomain = "{}.{}".format(extracted.domain.encode('utf-8'), extracted.suffix.encode('utf-8'))
        appUrl[secdomain].add(app)
    fw = open(outputfile, 'w')
    for k,v in appUrl.iteritems():
        if len(v) == 1:
            fw.write("%s\t%s\n" % (k, v))
    fw.close()

def statUrlPcapCoverage(tbl):
    print 'start'
    pkgs = load_pkgs(DB=tbl)
    urls = set()
    sqldao = SqlDao()
    for app, url,_ in sqldao.execute('SELECT * FROM url_apk'):
        urls.add(url.replace('http://', '').replace('www.', ''))
    total = 0
    contain = 0
    for pkg in pkgs:
        url = pkg.host + '/' + pkg.path
        url = url.replace('//','/')
        if len(pkg.queries) == 0:
            total += 1
            if url in urls:
                contain += 1
            else:
                print url.encode('utf-8')
    print "Tbl: %s\tTotal: %s\t Contain: %s" % (tbl, total, contain)

def statFile():
    from utils import load_appinfo, longest_common_substring, get_top_domain
    from classifier import header_classifier
    def loadExpApp():
        expApp=set()
        for app in open("resource/exp_app.txt"):
            expApp.add(app.strip().lower())
        return expApp
    expApp = loadExpApp()
    sqldao = SqlDao()
    fileApp = defaultdict(set)
    fileUrl = defaultdict(set)
    urlApp = defaultdict(set)
    substrCompany = defaultdict(set)
    appCompany, appName = load_appinfo()
    for app, url, fileName in sqldao.execute('SELECT * FROM url_apk'):
        app = app.lower()
        url = url.replace('http://', '').replace('www.','').replace('-', '.').split('/')[0].split(':')[0]
        topDomain = get_top_domain(url)
        fileApp[fileName].add(appCompany[app])
        fileUrl[fileName].add(url)
        urlApp[url].add(app)
        topDomain = get_top_domain(url)
        urlApp[topDomain].add(app)
        common_str_pkg = longest_common_substring(url.lower(), app)
        substrCompany[common_str_pkg].add(appCompany[app])
        common_str_company = longest_common_substring(url.lower(), appCompany[app].lower())
        substrCompany[common_str_company].add(appCompany[app])
        common_str_name = longest_common_substring(url.lower(), appName[app].lower())
        substrCompany[common_str_name].add(appCompany[app])

    for tbl in ['packages_20150429', 'packages_20150509', 'packages_20150526']:
        for pkg in load_pkgs(DB = tbl):
            app = pkg.app
            if app not in appCompany:
                continue
            
            url = pkg.host.replace('http://', '').replace('www.','').replace('-', '.').split('/')[0].split(':')[0]
            topDomain = get_top_domain(url)
            urlApp[topDomain].add(app)
            urlApp[url].add(app)
            common_str = longest_common_substring(url.lower(), app)
            substrCompany[common_str].add(appCompany[app])
            common_str_company = longest_common_substring(url.lower(), appCompany[app].lower())
            substrCompany[common_str_company].add(appCompany[app])
            common_str_name = longest_common_substring(url.lower(), appName[app].lower())
            substrCompany[common_str_name].add(appCompany[app])
            if topDomain == 'maps.moovitapp.com':
                print '#TOPDOMAIN'

    rmdUrls = set()

    for fileName,urls in fileApp.iteritems():
        if len(urls) > 1:
            for url in fileUrl[fileName]:
                rmdUrls.add(url)
    ########################
    # Generate Rules
    ########################
    covered = set()
    rules = {}
    for url, apps in urlApp.iteritems():
        if url == 'flixster.com':
            print '#', url in rmdUrls
            print '#', len(apps)
            print apps
        if url not in rmdUrls and len(apps) == 1:
            app = apps.pop()
            for astr in [app, appCompany[app], appName[app]]:
                common_str = longest_common_substring(url.lower(), astr.lower())
                if url == 'flixster.com':
                    print common_str
                    print substrCompany[common_str]
                if len(substrCompany[common_str]) < 5 and app in expApp:
                    covered.add(app)
                    rules[url] = app
                    if url == 'flixster.com':
                        print 'INNNNNNNNNNNN'

    ##################
    # Test
    ##################
    pkgs = load_pkgs(DB='packages_20150210')
    correct = 0
    total = 0
    apps = set()
    uncovered_urls = defaultdict(set)
    url_counter = defaultdict(int)
    def classify(url):
        if url in rules:
            return rules[url]
        return None
    noQuery = 0
    for pkg in pkgs:
        if len(pkg.queries) > 0 or pkg.app not in expApp or header_classifier(pkg) != None:
            continue
        noQuery += 1
        host = pkg.host.replace('-','.')
        secdomain = pkg.secdomain.replace('-', '.')
        app = classify(host)
        app = classify(secdomain) if app == None else app
        if app:
            total += 1
            if app == pkg.app:
                correct += 1
            else:
                print pkg.host, pkg.app, rules[host] if host in rules else rules[secdomain]
        else:
            uncovered_urls[pkg.host].add(pkg.app)
            url_counter[pkg.host] += 1
    print 'Total: %s Correct: %s No Query: %s' % (total, correct, noQuery)
    #for url in filter(lambda url: len(uncovered_urls[url]) == 1, url_counter):
    #    print '%s\t%s\t%s' % (url, url_counter[url], uncovered_urls[url])
    

if __name__ == '__main__':
  if sys.argv[1] == 'rmOtherApp':
    if len(sys.argv) > 2:
      rmOtherApp(sys.argv[2:])
    else:
      rmOtherApp()
  elif sys.argv[1] == 'batchTest':
    batchTest(sys.argv[2])
  elif sys.argv[1] == 'stat':
      statUrlPcap(sys.argv[2])
  else:
      #statUrlPcapCoverage(sys.argv[1])
      statFile()
