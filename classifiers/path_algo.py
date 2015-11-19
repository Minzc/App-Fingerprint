from sqldao import SqlDao
from nltk import FreqDist
from utils import app_clean


class TreeNode:
    def __init__(self, father, value):
        self.children = []
        self.father = [father]
        self.value = value
        self.status = 1
        self.counter = 0
        self.leaf = {}
        self.label = FreqDist()

    def inc_label(self, label):
        self.label.inc(label)

    def inc_counter(self):
        self.counter += 1

    def get_counter(self):
        return self.counter

    def get_value(self):
        return self.value

    def add_child(self, treenode):
        self.children.append(treenode)

    def get_child(self, value):
        for child in self.children:
            if child.value == value:
                return child
        return None

    def get_all_child(self):
        return self.children

    def get_father(self):
        return self.father

    def set_status(self, status):
        self.status = status

    def get_status(self):
        return self.status

    def to_string(self):
        return ','.join([child.get_value() for child in self.children])

    def add_leaf(self, node):
        self.leaf[node.get_value()] = node

    def get_all_leaf(self):
        return self.leaf

    def add_father(self, node):
        self.father.append(node)


def _add_node(root, tree_path, label):
    tree_node = root

    for i in range(len(tree_path)):
        node_value = tree_path[i]
        child_node = tree_node.get_child(node_value)
        # not adding leaf node
        if child_node == None:
            # not leaf node
            child_node = TreeNode(tree_node, node_value)
            tree_node.add_child(child_node)

        child_node.inc_counter()
        child_node.inc_label(label)
        tree_node = child_node


def host_tree(train_set=None):

    root = TreeNode(None, None)
    c = 0

    if not train_set:
        train_set = _get_train_set()

    for package in train_set:
        if not package.company or 'X-Requested-With' in package.add_header:
            continue
        tree_path = [package.host, package.app + '$' + package.company, package.path]
        _add_node(root, tree_path, package.company)

    QUERY = 'INSERT INTO rules (hst, path, company, app) VALUES (%s,%s,%s,%s)'
    for hstnode in root.get_all_child():
        companies = set()
        packages = set()
        for appnode in hstnode.get_all_child():
            app, company = appnode.get_value().split('$')
            companies.add(company)
            packages.add(app_clean(app).split('.')[-1])

        records = []
        for appnode in hstnode.get_all_child():
            for pathnode in appnode.get_all_child():
                for i in range(pathnode.get_counter()):
                    features = [p for p in pathnode.get_value().split('/') if len(p) > 0]
                    records.append([appnode.get_value(), features])
        rules = pathtree(records, tfidf)
        company = '$'.join(companies)

        if (len(packages) == 1 or len(companies) == 1):
            sqldao.execute(QUERY, (hstnode.get_value(), '', company, ''))
        else:
            maxlabel = hstnode.label.max()
            if hstnode.label[maxlabel] * 1.0 / sum(hstnode.label.values()) >= 0.9:
                sqldao.execute(QUERY, (hstnode.get_value(), '', maxlabel, ''))

        for f, app in rules.items():
            app, company = app.split('$')
            sqldao.execute(QUERY, (hstnode.get_value(), f, company, app))
    sqldao.close()


def pathtree(records, tfidf):
    """
	[label, (feature1, feature2, feature3)]
	"""
    root = TreeNode(None, None)
    for record in records:
        _add_node(root, record[1], record[0])

    queue = root.get_all_child()
    rules = {}

    while len(queue):
        node = queue[0]
        queue = queue[1:]
        sumv = sum(node.label.values())
        maxlabel = node.label.max()
        if node.label[maxlabel] == sumv:
            app, company = maxlabel.split('$')
            bestf = node.get_value()
            score = tfidf[bestf][app]
            while (len(node.get_all_child()) == 1):
                f = node.get_all_child()[0].get_value()
                if score < tfidf[f][app]:
                    score = tfidf[f][app]
                    bestf = f
                node = node.get_all_child()[0]
            rules[bestf] = maxlabel
        elif node.label[maxlabel] > sumv * 0.9:
            rules[node.get_value()] = maxlabel
        else:
            for child in node.get_all_child():
                queue.append(child)
    return rules


if __name__ == '__main__':
    host_tree()
