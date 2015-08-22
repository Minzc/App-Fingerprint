from classifiers.agent_classifier import AgentClassifier
from classifiers.algo import KVClassifier
from classifiers.classifier import HeaderClassifier
from classifiers.host import HostApp
from classifiers.fp import CMAR
import consts

def classifier_factory(names, appType):
  classifiers = []
  for name in names:
    if name == consts.HEAD_CLASSIFIER:
      classifier = HeaderClassifier()
    elif name == consts.AGENT_CLASSIFIER:
      classifier = AgentClassifier()
    elif name == consts.HOST_CLASSIFIER:
      classifier = HostApp(appType)
    elif name == consts.CMAR_CLASSIFIER:
      classifier = CMAR(min_cover = 3)
    elif name == consts.KV_CLASSIFIER:
      classifier = KVClassifier(appType)
    classifiers.append((name, classifier))
  return classifiers

