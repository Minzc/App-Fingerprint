from sqldao import SqlDao
from package import Package


class HeadRuler:
    def classify(self, package):
        identifier = ['x-umeng-sdk', 'x-vungle-bundle-id', 'x-requested-with']
        for id in identifier:
            for head_seg in package.add_header.split('\n'):
                if id in head_seg:
                    pkgname = head_seg.split(':')[1].strip()
                    if '.' in pkgname:
                        return pkgname
        return None
    def loadRules(self):
        pass


def test_header():
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
    head_classifier = HeadRuler()
    counter = 0
    correct = 0
    for record in records:
        rst = head_classifier.classify(record)
        if rst:
            counter += 1
            if rst in record.app:
                correct += 1
            else:
                print rst, '#', record.app
            sqldao.execute('UPDATE packages SET classified = %s WHERE id = %s', (1, record.id))
    print counter, correct


if __name__ == '__main__':
    import sys

    def help():
        print 'header.py test\ttest agent features on db'

    if len(sys.argv) < 2:
        help()
    elif sys.argv[1] == 'test':
        test_header()
    else:
        help()
