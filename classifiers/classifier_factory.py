from classifiers.agent import AgentClassifier
from classifiers.algo import KVClassifier
from classifiers.head import HeaderClassifier
import const.consts as consts
from classifiers.uri import UriClassifier
from classifiers.fp import CMAR


def classifier_factory(names, appType):
    classifiers = []
    for name in names:
        if name == consts.HEAD_CLASSIFIER:
            classifier = HeaderClassifier()
        elif name == consts.AGENT_CLASSIFIER:
            classifier = AgentClassifier(inferFrmData=True)
        elif name == consts.URI_CLASSIFIER:
            # classifier = UriClassifier(appType)
            classifier = KVClassifier(appType, consts.PATH_MINER)
        elif name == consts.KV_CLASSIFIER:
            classifier = KVClassifier(appType, consts.KV_MINER)
        elif name == consts.CMAR_CLASSIFIER:
            classifier = CMAR()
        classifiers.append((name, classifier))
    return classifiers
