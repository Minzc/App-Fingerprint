#!/usr/bin/env python
# -*- coding: UTF-8 -*-
# 
	
import re
import string
import tldextract
from sqldao import SqlDao
from package import Package
import random

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

def load_tfidf():
	sqldao = SqlDao()
	QUERY = 'SELECT word, app, tfidf FROM tfidf'
	relation = {}
	for word, app, tfidf in sqldao.execute(QUERY):
		word,app = lower_all((word, app))
		relation.setdefault(word,{})
		relation[word][app] = tfidf
	return relation

def load_pkgs(limit = None, filterFunc=lambda x : True, DB="packages"):
    records = []
    sqldao = SqlDao()
    QUERY = None
    if not limit:
        QUERY = "select id, app, add_header, path, refer, hst, agent, company,name, dst, raw from %s where method=\'GET\'" % DB
    else:
        QUERY = "select id, app, add_header, path, refer, hst, agent, company,name, dst, raw from %s where method=\'GET\' limit %s" % (DB, limit)
    print QUERY

    for id, app, add_header, path, refer, host, agent, company,name, dst, raw in sqldao.execute(QUERY):
        
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
       
        if filterFunc(package):
            records.append(package)
    return records

def get_record_f(record):
    """Get package features"""
    features = filter(None, record.path.split('/'))
    # queries = record.querys
    # for k, vs in filter(None, queries.items()):
    #     if len(k) < 2:
    #         continue
    #     features.append(k)
    #     for v in vs:
    #         if len(v) < 2:
    #             continue
    #         features.append(v.replace(' ', '').replace('\n', ''))

    for head_seg in filter(None, record.add_header.split('\n')):
        if len(head_seg) > 2:
            features.append(head_seg.replace(' ', '').strip())

    for agent_seg in filter(None, record.agent.split(' ')):
        if len(agent_seg) < 2:
          features.append(agent_seg.replace(' ', ''))
    host = record.host if record.host else record.dst
    features.append(host)
    features.append(record.app)

    return features


from nltk import FreqDist
class Relation:	
	def __init__(self):
		self.counter = {}
	def add(self, key, value):
		if key not in self.counter:
			self.counter[key] = FreqDist()
		self.counter[key].inc(value)
	
	def addCnt(self, key, value, count):
		if key not in self.counter:
			self.counter[key] = FreqDist()
		self.counter[key].inc(value,count)

	def get(self):
		return self.counter

	def addall(self, key, values):
		for value in values:
			self.add(key, value)
