from classifiers.agent import AgentClassifier
from classifiers.algo import KVClassifier
from classifiers.head import HeaderClassifier
from classifiers.host import HostApp
from classifiers.fp_modify import CMAR
from classifiers.path_algo import PathApp
import const.consts as consts
from classifiers.uri import UriClassifier


def classifier_factory(names, appType):
    classifiers = []
    for name in names:
        if name == consts.HEAD_CLASSIFIER:
            classifier = HeaderClassifier()
        elif name == consts.AGENT_CLASSIFIER:
            classifier = AgentClassifier(inferFrmData=True, sampleRate=1)
        elif name == consts.URI_CLASSIFIER:
            classifier = UriClassifier(appType)
        elif name == consts.KV_CLASSIFIER:
            classifier = KVClassifier(appType, inferFrmData=True, sampleRate=1)
        classifiers.append((name, classifier))
    return classifiers
