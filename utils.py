#!/usr/bin/env python
# -*- coding: UTF-8 -*-
# 
	
import re
import string
import tldextract
from sqldao import SqlDao
import re
from package import Package
import random

regex_enpunc = re.compile("[" + string.punctuation + "]")

extract = tldextract.TLDExtract(
    suffix_list_url="source/effective_tld_names.dat.txt",
    cache_file=False)

max_wordlen, min_wordlen = 100, 2

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

def load_dict():
    sqldao = SqlDao()
    dic = set()
    for feature in sqldao.execute('SELECT feature FROM appfeatures'):
    	dic.add(feature[0].lower())
    return dic


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

def load_pkgs(limit = None):
	records = []
	sqldao = SqlDao()
	QUERY = None
	if not limit:
		QUERY = "select id, app, add_header, path, refer, hst, agent, company,name, dst from packages where httptype=0"
	else:
		QUERY = "select id, app, add_header, path, refer, hst, agent, company,name, dst from packages where httptype=0 limit " + str(limit)
	for id, app, add_header, path, refer, host, agent, company,name, dst in sqldao.execute(QUERY):
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
		records.append(package)
	return records

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
