from collections import defaultdict
import sys
import utils
class AdService:
    def __init__(self):
        print '>>> Start initializing miner'
        ad_dict = defaultdict(list)
        id_keys = set()
        for k, v in [ln.strip().split(':') for ln in open('ad_dict.txt')]:
            id_keys.add(v)
            ad_dict[v].append(k)
        self.ad_dict = ad_dict
        self.id_keys = id_keys

    def ad_service(self, package):
        for k, v in package.querys.iteritems():
            for host in self.ad_dict[k]:
                if host in package.host:
                    yield (package.secdomain, k, v[0], package.app)

    def _identify_keys(self, package):
        for k, v in package.querys.iteritems():
            if package.app in v:
                yield (package.host, k, v, app)


if __name__ == '__main__':
    miner = AdService()
    print '>>> Finish Initializing Miner'
    if sys.argv[1] == 'test':
        kv_rules = set()
        for package in utils.load_pkgs():
            for r in miner.ad_service(package):
                kv_rules.add(r)
        
        for tuples in kv_rules:
            print tuples

        print '>>> number of apps', len({app for _,_,_,app in kv_rules})
    elif sys.argv[1] == 'findkey':
        adRules = set()
        for package in utils.load_pkgs():
            for r in miner.ad_service(package):
                adRules.add(r)
        for rst in adRules:
            print rst
