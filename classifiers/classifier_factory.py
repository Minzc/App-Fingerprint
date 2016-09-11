from classifiers.agent import AgentClassifier
from classifiers.agent_baseline import AgentBLClassifier
from classifiers.agent_pattern import AgentBoundary
from classifiers.algo import QueryClassifier
from classifiers.algo_baseline import BaseLineClassifier
import const.consts as consts


def classifier_factory(names, appType):
    classifiers = []
    for name in names:
        if name == consts.HEAD_CLASSIFIER:
            classifier = QueryClassifier(appType, consts.Head_MINER)
        elif name == consts.AGENT_CLASSIFIER:
            classifier = AgentClassifier(inferFrmData=True)
        elif name == consts.URI_CLASSIFIER:
            classifier = QueryClassifier(appType, consts.PATH_MINER)
        elif name == consts.KV_CLASSIFIER:
            classifier = QueryClassifier(appType, consts.KV_MINER)
        elif name == consts.Query_BL_CLASSIFIER:
            classifier = BaseLineClassifier(appType, consts.KV_MINER)
        elif name == consts.Agent_BL_CLASSIFIER:
            classifier = AgentBLClassifier(inferFrmData=True)
        elif name == consts.AGENT_BOUNDARY_CLASSIFIER:
            classifier = AgentBoundary()
        classifiers.append((name, classifier))
    return classifiers

