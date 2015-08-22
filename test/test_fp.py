def _rever_map(mapObj):
    return {v: k for k, v in mapObj.items()}


def _load_data(filepath):
    records = []
    loadfile(filepath, lambda x: records.append(x.split(' ')))
    return records


def _load_appindx(filepath):
    apps = {}
    loadfile(filepath, lambda x: apps.setdefault(x.split('\t')[1], x.split('\t')[0]))
    return apps


def _load_findx(filepath):
    features = {}
    loadfile(filepath, lambda x: features.setdefault(x.split('\t')[1], x.split('\t')[0]))
    return features


def _load_records_hst(filepath):
    recordHost = []
    loadfile(filepath, lambda x: recordHost.append(x))
    return recordHost

def mining_fp_local(filepath, tSupport, tConfidence):
    """
    CMAR's local version. 
    This function is used to test the code localy
    Now this code can not work. To make it work again, need to modify the 
    function according to the database version
    """
    records = _load_data(filepath)
    appIndx = _load_appindx('app_index.txt')
    featureIndx = _load_findx('featureIndx.txt')
    recordHost = _load_records_hst('records_host.txt')

    # (feature, app, host index)
    rules = _gen_rules(records, tSupport, tConfidence)

    # feature, app, host
    rules = _prune_rules(rules, records)

    coverage = 0
    totalApp = set()
    for indx, record in enumerate(records):
        totalApp.add(record[-1])
        for feature in record[:-1]:
            if feature in rules:
                coverage += 1
                break
    coveredApp = set()
    for rule in rules:
        coveredApp.add(rule[1])
        rule = (featureIndx[rule[0]], appIndx[rule[1]], recordHost[rule[2]])

    print 'total:', len(records), 'coverage:', coverage, 'totalApp:', len(totalApp), 'coverApp:', len(coveredApp)


def revers():
    appIndx = load_appindx('app_index.txt')
    featureIndx = load_findx('featureIndx.txt')
