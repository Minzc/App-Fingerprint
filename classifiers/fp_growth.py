# encoding: utf-8

"""
A Python implementation of the FP-growth algorithm.

Basic usage of the module is very simple:

    >>> from fp_growth import find_frequent_itemsets
    >>> find_frequent_itemsets(transactions, minimum_support)
"""

from collections import defaultdict, namedtuple
from itertools import imap



__author__ = 'Eric Naeseth <eric@naeseth.com>'
__copyright__ = 'Copyright © 2009 Eric Naeseth'
__license__ = 'MIT License'

def find_frequent_itemsets(transactions, minimum_support, include_support=False, growth = lambda x : False):
    """
    Find frequent itemsets in the given transactions using FP-growth. This
    function returns a generator instead of an eagerly-populated list of items.

    The `transactions` parameter can be any iterable of iterables of items.
    `minimum_support` should be an integer specifying the minimum number of
    occurrences of an itemset for it to be accepted.

    Each item must be hashable (i.e., it must be valid as a member of a
    dictionary or a set).

    If `include_support` is true, yield (itemset, support) pairs instead of
    just the itemsets.
    """
    items = defaultdict(lambda: 0) # mapping from items to their supports
    processed_transactions = []

    # Load the passed-in transactions and count the support that individual
    # items have.
    for transaction in transactions:
        # processed = []
        # last item is class label
        for item in transaction[:-1]:
            items[item] += 1
        # processed_transactions.append(processed)
        processed_transactions.append(transaction)

    # Remove infrequent items from the item support dictionary.
    items = dict((item, support) for item, support in items.iteritems()
        if support >= minimum_support)
    
    # Build our FP-tree. Before any transactions can be added to the tree, they
    # must be stripped of infrequent items and their surviving items must be
    # sorted in decreasing order of frequency.
    def clean_transaction(transaction):
        # separate label and items
        transaction, label = transaction[:-1], transaction[-1]
        transaction = filter(lambda v: v in items, transaction)
        # sorted according to item's frequence
        # first by support then by itemnum
        transaction.sort(key=lambda v: (items[v], v), reverse=True)
        transaction = transaction + [label]
        return transaction

    master = FPTree()
    for transaction in imap(clean_transaction, processed_transactions):
        master.add(transaction)

    def find_with_suffix(tree, suffix):
        # guarantee the sequence of conditional item follows flist instead of random chosen.
        item_nodes = []
        for tuple in tree.items():
            item_nodes.append(tuple)
        item_nodes.sort(key=lambda v: (items[v[0]], v[0]))
        debug_num = 0


        for item, nodes in item_nodes:
            assert items[item] >= debug_num
            debug_num = items[item]

            support = 0
            support_dist = defaultdict(int)
            nodes = [node for node in nodes]

            for node in nodes:
                assert len(node.clsses) != 0
                support += node.count
                for k, v in node.clsses.items():
                    support_dist[k] += v

            if support >= minimum_support and item not in suffix:
                # New winner!
                found_set = [item] + suffix

                not_gen_rule = growth(found_set)
                if '[HOST]:meijer.122.2o7.net' in found_set:
                    print '[FPGROWTH103]', found_set, support_dist, support

                if not_gen_rule == False:
                    yield (found_set, support, support_dist) if include_support else found_set

                if len(support_dist) > 1 or not_gen_rule:
                    # Build a conditional tree and recursively search for frequent
                    # itemsets within it.
                    cond_tree = conditional_tree_from_paths(tree.prefix_paths(item),
                        minimum_support)
                    for s in find_with_suffix(cond_tree, found_set):
                        yield s # pass along the good news to our caller

            for node in nodes:
                if node.parent is not None: # the node might already be an orphan
                    assert len(node.children) <= 0
                    node.parent.remove(node)

    # Search for frequent itemsets, and yield the results we find.
    for itemset in find_with_suffix(master, []):
        yield itemset


class FPTree(object):
    """
    An FP tree.

    This object may only store transaction items that are hashable (i.e., all
    items must be valid as dictionary keys or set members).
    """

    Route = namedtuple('Route', 'head tail')

    def __init__(self):
        # The root node of the tree.
        self._root = FPNode(self, None, None)

        # A dictionary mapping items to the head and tail of a path of
        # "neighbors" that will hit every node containing that item.
        # Header Table
        self._routes = {}

    @property
    def root(self):
        """The root node of the tree."""
        return self._root

    def add(self, transactionNLabel):
        """
        Adds a transaction to the tree.
        """

        point = self._root
        transaction = transactionNLabel[:-1]

        for item in transaction:
            # point is FPNode
            next_point = point.search(item)
            if next_point:
                # There is already a node in this tree for the current
                # transaction item; reuse it.
                next_point.increment()
            else:
                # Create a new point and add it as a child of the point we're
                # currently looking at.
                next_point = FPNode(self, item)
                point.add(next_point)

                # Update the route of nodes that contain this item to include
                # our new node.
                self._update_route(next_point)

            point = next_point
        label = transactionNLabel[-1]
        point.assign_clss(label)

    def _update_route(self, point):
        """Add the given node to the route through all nodes for its item."""
        assert self is point.tree

        try:
            route = self._routes[point.item]
            route[1].neighbor = point # route[1] is the tail
            self._routes[point.item] = self.Route(route[0], point)
        except KeyError:
            # First node for this item; start a new route.
            self._routes[point.item] = self.Route(point, point)

    def items(self):
        """
        Generate one 2-tuples for each item represented in the tree. The first
        element of the tuple is the item itself, and the second element is a
        generator that will yield the nodes in the tree that belong to the item.
        """
        for item in self._routes.iterkeys():
            yield (item, self.nodes(item))

    def nodes(self, item):
        """
        Generates the sequence of nodes that contain the given item.
        """

        try:
            node = self._routes[item][0]
        except KeyError:
            return

        while node:
            yield node
            node = node.neighbor

    def prefix_paths(self, item):
        """Generates the prefix paths that end with the given item."""

        def collect_path(node):
            path = []
            while node and not node.root:
                path.append(node)
                node = node.parent
            path.reverse()
            return path

        return (collect_path(node) for node in self.nodes(item))

    def inspect(self):
        print 'Tree:'
        self.root.inspect(1)

        print
        print 'Routes:'
        for item, nodes in self.items():
            print '  %r' % item
            for node in nodes:
                print '    %r' % node

    def _removed(self, node):
        """Called when `node` is removed from the tree; performs cleanup."""

        head, tail = self._routes[node.item]
        if node is head:
            if node is tail or not node.neighbor:
                # It was the sole node.
                del self._routes[node.item]
            else:
                self._routes[node.item] = self.Route(node.neighbor, tail)
        else:
            for n in self.nodes(node.item):
                if n.neighbor is node:
                    n.neighbor = node.neighbor # skip over
                    if node is tail:
                        self._routes[node.item] = self.Route(head, n)
                    break

def conditional_tree_from_paths(paths, minimum_support):
    """Builds a conditional FP-tree from the given prefix paths."""
    tree = FPTree()
    condition_item = None
    items = set()
    itemcounter = defaultdict(lambda : 0)
    pathlsts = []

    for path in paths:
        pathlsts.append(path)
        for item in path:
            itemcounter[item.item] += path[-1].count

    itemcounter = dict((item, support) for item, support in itemcounter.iteritems()
                       if support >= minimum_support)

    # Import the nodes in the paths into the new tree. Only the counts of the
    # leaf notes matter; the remaining counts will be reconstructed from the
    # leaf counts.
    for path in pathlsts:
        if condition_item is None:
            condition_item = path[-1].item

        assert path[-1].item == condition_item
        point = tree.root
        condition_node = path[-1]
        count = condition_node.count

        for node in path[:-1]:
            if node.item not in itemcounter:
                continue
            next_point = point.search(node.item)
            if not next_point:
                # Add a new node to the tree.
                items.add(node.item)
                next_point = FPNode(tree, node.item, 0)
                point.add(next_point)
                tree._update_route(next_point)

            next_point._count += count
            point = next_point

        # copy classes distribution
        if len(path) > 1:
            assert len(condition_node.clsses) > 0

        for k, v in condition_node.clsses.items():
            point.assign_clss(k, v)

    assert condition_item is not None

    # Calculate the counts of the non-leaf nodes.
    # for path in tree.prefix_paths(condition_item):
    #     count = path[-1].count
    #     for node in reversed(path[:-1]):
    #         node._count += count

    # Eliminate the nodes for any items that are no longer frequent.
    for item in items:
        support = sum(n.count for n in tree.nodes(item))
        assert support >= minimum_support
        # if support < minimum_support:
        #     # Doesn't make the cut anymore
        #     for node in tree.nodes(item):
        #         if node.parent is not None:
        #             node.parent.remove(node)

    # Finally, remove the nodes corresponding to the item for which this
    # conditional tree was generated.
    # for node in tree.nodes(condition_item):
    #     if node.parent is not None: # the node might already be an orphan
    #         node.parent.remove(node)

    return tree

class FPNode(object):
    """A node in an FP tree."""

    def __init__(self, tree, item, count=1):
        self._tree = tree
        self._item = item
        self._count = count
        self._parent = None
        self._children = {}
        self._neighbor = None
        self._clsses = defaultdict(int)


    def assign_clss(self, clss, count=1):
        """set class labels"""
        self._clsses[clss] += count


    def add(self, child):
        """Adds the given FPNode `child` as a child of this node."""

        if not isinstance(child, FPNode):
            raise TypeError("Can only add other FPNodes as children")

        if not child.item in self._children:
            self._children[child.item] = child
            child.parent = self

    def search(self, item):
        """
        Checks to see if this node contains a child node for the given item.
        If so, that node is returned; otherwise, `None` is returned.
        """

        try:
            return self._children[item]
        except KeyError:
            return None

    def remove(self, child):
        try:
            if self._children[child.item] is child:
                del self._children[child.item]
                child.parent = None
                self._tree._removed(child)

                assert len(child.clsses) > 0
                assert len(child.children) <= 0

                # Since the parent node only has projected distribution
                # merge class distribution into parent node
                for k, v in child.clsses.items():
                    self.assign_clss(k, v)

                if len(child.children) > 0:
                    print child
            else:
                raise ValueError("that node is not a child of this node")
        except KeyError:
            raise ValueError("that node is not a child of this node")

    def __contains__(self, item):
        return item in self._children

    @property
    def clsses(self):
        """The class distritbuion of this node"""
        return self._clsses

    @property
    def tree(self):
        """The tree in which this node appears."""
        return self._tree

    @property
    def item(self):
        """The item contained in this node."""
        return self._item

    @property
    def count(self):
        """The count associated with this node's item."""
        return self._count

    def increment(self):
        """Increments the count associated with this node's item."""
        if self._count is None:
            raise ValueError("Root nodes have no associated count.")
        self._count += 1

    @property
    def root(self):
        """True if this node is the root of a tree; false if otherwise."""
        return self._item is None and self._count is None

    @property
    def leaf(self):
        """True if this node is a leaf in the tree; false if otherwise."""
        return len(self._children) == 0

    def parent():
        doc = "The node's parent."
        def fget(self):
            return self._parent
        def fset(self, value):
            if value is not None and not isinstance(value, FPNode):
                raise TypeError("A node must have an FPNode as a parent.")
            if value and value.tree is not self.tree:
                raise ValueError("Cannot have a parent from another tree.")
            self._parent = value
        return locals()
    parent = property(**parent())

    def neighbor():
        doc = """
        The node's neighbor; the one with the same value that is "to the right"
        of it in the tree.
        """
        def fget(self):
            return self._neighbor
        def fset(self, value):
            if value is not None and not isinstance(value, FPNode):
                raise TypeError("A node must have an FPNode as a neighbor.")
            if value and value.tree is not self.tree:
                raise ValueError("Cannot have a neighbor from another tree.")
            self._neighbor = value
        return locals()
    neighbor = property(**neighbor())

    @property
    def children(self):
        """The nodes that are children of this node."""
        return tuple(self._children.itervalues())

    def inspect(self, depth=0):
        print ('  ' * depth) + repr(self)
        for child in self.children:
            child.inspect(depth + 1)

    def __repr__(self):
        if self.root:
            return "<%s (root)>" % type(self).__name__
        return "<%s %r (%r)>" % (type(self).__name__, self.item, self.count)


if __name__ == '__main__':
    from optparse import OptionParser
    import csv

    p = OptionParser(usage='%prog data_file')
    p.add_option('-s', '--minimum-support', dest='minsup', type='int',
        help='Minimum itemset support (default: 2)')
    p.set_defaults(minsup=2)

    options, args = p.parse_args()
    if len(args) < 1:
        p.error('must provide the path to a CSV file to read')

    f = open(args[0])
    try:
        for itemset, support, clsses in find_frequent_itemsets(csv.reader(f), options.minsup, True):
            print '{' + ', '.join(itemset) + '} ' + str(support), clsses
    finally:
        f.close()
