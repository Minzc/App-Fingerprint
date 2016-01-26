from classifiers.agent import AgentClassifier
from classifiers.agent_bl import AgentBLClassifier
from classifiers.algo import QueryClassifier
from classifiers.baseline import BaseLineClassifier
from classifiers.head import HeaderClassifier
import const.consts as consts
from classifiers.fp import CMAR


def classifier_factory(names, appType):
    classifiers = []
    for name in names:
        if name == consts.HEAD_CLASSIFIER:
            classifier = HeaderClassifier()
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
        elif name == consts.CMAR_CLASSIFIER:
            classifier = CMAR()
        classifiers.append((name, classifier))
    return classifiers

