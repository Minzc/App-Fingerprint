# -*- coding:utf-8 -*-
import sys

from nltk import FreqDist

from utils import loadfile, Relation, top_domain, lower_all, app_clean, load_pkgs
from sqldao import SqlDao
from package import Package
from collections import defaultdict, namedtuple

def stat_path():
    sqldao = SqlDao()
    counter = FreqDist()
    records = load_pkgs()

    count = 0
    l_max = 0
    total = 0
    for pk in records:
        counter.inc(pk.app)
    # l = set()
    # pathsegs = filter(None,pk.path.split('/'))
    # for i in pathsegs:
    # if len(i) < 2:
    # 		continue
    # 	l.add(i.replace(' ',''))
    # 	counter.inc(i.replace(' ',''))

    # queries = pk.querys
    # for k, vs in filter(None,queries.items()):
    # 	if len(k) < 2:
    # 		continue
    # 	counter.inc(k.replace(' ',''))
    # 	l.add(k.replace(' ',''))
    # 	for v in vs:
    # 		if len(v) < 2:
    # 			continue
    # 		l.add(v.replace(' ',''))
    # 		counter.inc(v.replace(' ','').replace('\n',''))

    # for head_seg in filter(None,pk.add_header.split('\n')):
    # 	if len(head_seg) < 2:
    # 		continue
    # 	l.add(head_seg.replace(' ','').replace(' ','').strip())
    # 	counter.inc(head_seg.replace(' ','').replace(' ','').strip())

    # for agent_seg in filter(None,pk.agent.split(' ')):
    # 	if len(agent_seg) < 2:
    # 		continue
    # 	l.add(agent_seg.replace(' ','').replace(' ',''))
    # 	counter.inc(agent_seg.replace(' ',''))
    # l_max = max(l_max, len(l))
    # total += len(l)

    for k, v in counter.items():
        print v


def stat_add_header():
    sqldao = SqlDao()
    counter = Relation()
    for app, header in sqldao.execute('select app, add_header from packages where httptype = \'0\' '):
        headers = {h.split(':')[0] for h in header.split('\n')}
        for h in headers:
            counter.add(h, app)
    sort = FreqDist()
    for k, v in counter.get().items():
        sort.inc(k, len(v))
    for k, v in sort.items():
        print k, v


# stat_add_header()

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


def stat_relation(filepath, col1, col2, outfile):
    """
    Number of times col1 and col2 co-occure
    """


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
        for k, v in package.querys.items():
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
            for k in record.querys:
                if record.app in record.querys[k]:
                    parameters.add(k)
    apps = set()
    pkgNum = 0
    for record in records:
        for parameter in parameters:
            if parameter in record.querys:
                print '1', record.querys[parameter]
                if record.app not in record.querys[parameter]:
                    print '2', record.app, record.querys[parameter], parameter, record.origPath
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
    appComponay = {}
    appName = {}
    for record in records:
        relation.add(record.host, record.app + '$' + none2str(record.company) + '$' + none2str(record.name))
        appComponay[record.app] = record.company
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
      for k, v in package.querys.items():
        print k, v

def test_client_slot():
  records = load_pkgs()
  relations = defaultdict(set)
  for record in records:
    if 'client' in record.querys and 'slotname' in record.querys:
      client = record.querys['client'][0]
      slotname = record.querys['slotname'][0]
      relations[client + '$' + slotname].add(record.app)

  print len(relations)
  for k, v in relations.iteritems():
    if len(v) > 1:
      print k, v

def test_sdk_id():
  records = load_pkgs()
  relations = defaultdict(set)
  for record in records:
    if 'id' in record.querys and 'ads.mopub.com' == record.host:
      appid = record.querys['id'][0]
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
  tbls = ["packages_20150210", "packages_20150429", "packages_20150509", "packages_20150526"]
  counter = defaultdict(lambda : defaultdict( lambda : defaultdict( lambda : defaultdict(set))))
  valueCounter = defaultdict(set)
  hostTokenCounter = defaultdict(int)
  for tbl in tbls:
    print tbl
    pkgs = load_pkgs(None, DB = tbl)
    for pkg in pkgs:
      for k,v in pkg.querys.items():
        map(lambda x : counter[pkg.secdomain][pkg.app][k][x].add(tbl), v)
        map(lambda x : valueCounter[x].add(pkg.app), v)
  fw = open(outputfile, 'w')
  score = defaultdict(lambda : defaultdict(lambda : {'app':set(), 'score':0}))
  for secdomain in counter:
    for app in counter[secdomain]:
      for k in counter[secdomain][app]:
        for v in counter[secdomain][app][k]:
          if len(valueCounter[v]) == 1:
            score[secdomain][k]['score'] += len(counter[secdomain][app][k])
            score[secdomain][k]['app'].add(app)
            try:
              fw.write("%s %s %s %s\n" % (secdomain, app, k, v.replace('\n','').replace(' ', ''), len(counter[secdomain][app][k])))
            except:
              pass
  fw.close()
  fw = open(outputfile+".score", 'w')
  for secdomain in score:
    for key in score[secdomain]:
      fw.write("%s\t%s\t%s\t%s\n" % (secdomain, key, score[secdomain][key]['score'], len(score[secdomain][key]['app'])))
  fw.close()

if __name__ == '__main__':
  print sys.argv[1]
  if sys.argv[1] == 'rmOtherApp':
    if len(sys.argv) > 2:
      rmOtherApp(sys.argv[2:])
    else:
      rmOtherApp()
  elif sys.argv[1] == 'batchTest':
    batchTest(sys.argv[2])
