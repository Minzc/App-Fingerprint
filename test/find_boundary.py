from __future__ import absolute_import, division, print_function, unicode_literals
import re
from collections import defaultdict


def process_agent(agent, app):
    agent = agent.replace("%20", " ")
    agent = re.sub(r'[a-z]?[0-9]+-[a-z]?[0-9]+-[a-z]?[0-9]+', r'[VERSION]', agent)
    agent = re.sub(r'(/)([0-9]+)([ ;])', r'\1[VERSION]\3', agent)
    agent = re.sub(r'/[0-9][.0-9]+', r'/[VERSION]', agent)
    agent = re.sub(r'([ :v])([0-9][.0-9]+)([ ;),])', r'\1[VERSION]\3', agent)
    agent = re.sub(r'([ :v])([0-9][_0-9]+)([ ;),])', r'\1[VERSION]\3', agent)
    agent = re.sub(r'(^[0-9a-z]*)(.' + app + r'$)', r'[RANDOM]\2', agent)
    agent = agent.replace('/', ' / ')
    agent = agent.replace('(', ' ( ')
    agent = agent.replace(')', ' ) ')
    agent = agent.replace(';', ' ; ')
    return agent


if True:
    db = [
        ("e f a b c", "A"),
        ("g h e a b c", "B"),
        ("o d m n a b", "C"),
        ("k d q n a b", "D"),
        ("a e f b", "A"),
        ("a g h e b", "B"),

    ]
    support_t = 1
else:
    db = set()
    from sqldao import SqlDao

    sqldao = SqlDao()
    for app, agent in sqldao.execute("select distinct app, agent from ios_packages_2015_08_10"):
        db.add((agent, app))
    support_t = 10

# SPLITTER = re.compile("[" + r'''!"#$%&'()*+,\:;<=>?@[\]^`{|}~ ''' + "]")
SPLITTER = re.compile("[" + r'''!"#$%&'*+,:<=>?@[\]^`{|}~ ''' + "]")

db = [(['^'] + filter(None, SPLITTER.split(process_agent(x[0], x[1]))) + ['$'], x[1]) for x in db]

frequent_items = set()
counter = defaultdict(set)
for (t, c) in db:
    for w in t:
        counter[w].add(c)
for (w, c) in counter.items():
    if len(c) > 2:
        frequent_items.add(w)

result = {}
rules = {}



def cal_average_idf(signature):
    t = 0
    for i in signature:
        t += 1 / len(counter[i])
    return t / len(signature)


def mine_suffix(prefix, suffix, mdb, gap, parF1):
    occurs = defaultdict(list)
    signatures = defaultdict(set)
    support = len({i for (i, _, _) in mdb}) if len(suffix) != 0 else 0
    tmp = set()
    for (i, prefix_pos, startpos) in mdb:
        seq, app = db[i]
        signature = tuple(seq[prefix_pos + 1: startpos - len(suffix)])
        if len(suffix) > 0:
            tmp.add((i, tuple(prefix), tuple(suffix), signature))
            signatures[signature].add(app)

        for j in xrange(startpos, len(seq)):
            if j - startpos <= gap and seq[j] in frequent_items:
                l = occurs[seq[j]]
                l.append((i, prefix_pos, j + 1))

    cf = check_signature(signatures)
    f1 = insert_context(cf, prefix, suffix, support, parF1)
    tmp = filter(lambda x: (x[1], x[2]) in result, tmp)

    # for i, p, s, signature in tmp:
    #     if i not in rules \
    #             or rules[i][3] < result[(p, s)][2] \
    #             or (rules[i][3] == result[(p, s)][2] and len(rules[i][2]) > len(signature)):
    #         if len(signatures[signature]) == 1:
    #             rules[i] = (p, s, signature, result[(p, s)][2])
    for i, p, s, signature in tmp:
        idf = cal_average_idf(signature)
        context_score = result[(p, s)][2]
        fuse_score = idf * context_score / (idf + context_score)
        if i not in rules \
                or rules[i][3] < fuse_score \
                or (rules[i][3] == fuse_score and len(rules[i][2]) > len(signature)):
            if len(signatures[signature]) == 1:
                rules[i] = (p, s, signature, fuse_score)

    maxSuffixSupport = 0
    if cf < 1:
        for (c, newmdb) in occurs.iteritems():
            childSupport = len({i for (i, _, _) in newmdb})
            maxSuffixSupport = max(childSupport, maxSuffixSupport)
            if childSupport > support_t:
                mine_suffix(prefix, suffix + [c], newmdb, 0, max(f1, parF1))
    return maxSuffixSupport


def insert_context(cf, prefix, suffix, support, parF1):
    if len(suffix) > 0:
        support = support / len(db)
        f1 = support * cf / (support + cf)
        if f1 > parF1:
            result[(tuple(prefix), tuple(suffix))] = (support, cf, support * cf / (support + cf))
        #TODO
        result[(tuple(prefix), tuple(suffix))] = (support, cf, support * cf / (support + cf))
        return f1
    return 0


def check_signature(signatures):
    t = 0
    for signature in signatures:
        t += cal_average_idf(signature)
    return 0 if len(signatures) == 0 else t / len(signatures)


def mine_rec(prefix, mdb, gap, expSuffix):
    occurs = defaultdict(list)

    for (i, startpos) in mdb:
        seq = db[i][0]
        for j in xrange(startpos, -1, -1):
            if startpos - j <= gap and seq[j] in frequent_items:
                l = occurs[seq[j]]
                l.append((i, j - 1))

    if expSuffix:
        suffix_mdb = [
            # dbindex, last position of prefix, start position of suffix
            (i, startpos + len(prefix) + 1, startpos + len(prefix) + 3) for (i, startpos) in mdb if startpos + len(prefix) + 3 < len(db[i][0])
            ]
        if prefix == ['c']:
                pass
        if mine_suffix(prefix, [], suffix_mdb, 10000, 0) < support_t:
            return

    support = len({i for (i, _) in mdb}) if len(prefix) != 0 else 0
    for c, newmdb in occurs.items():
        childSupport = len({i for (i, _) in newmdb})
        if childSupport > support_t:
            # mine_rec([c] + prefix, newmdb, 0, childSupport != support)
            mine_rec([c] + prefix, newmdb, 0, True)


mine_rec([], [(i, len(db[i][0]) - 1) for i in range(len(db))], 10000, False)

print(len(result))
for a, b in sorted(result.items(), key=lambda x: x[1][2]):
    print(a, b)
print('#' * 10)
for r in rules.values():
    print(r)
covered_apps = set()
for k in rules.keys():
    covered_apps.add(db[k][1])
print("Covered Apps", len(covered_apps))
