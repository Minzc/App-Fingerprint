from collections import namedtuple

Prediction = namedtuple('Prediction', 'label, score, evidence')
QueryKey = namedtuple('Rule', 'secdomain, key, score, labelNum, hostNum')
Rule = namedtuple('Rule', 'host, prefix, identifier, suffix, score, label')

NULLPrediction = Prediction(None, -1, None)

LABEL = 'l'
ERROR = 'e'
SCORE = 's'
SUPPORT = 't'
REGEX_OBJ = 'r'
EVIDENCE = '1'

#############TYPE###############
APP_RULE = 0
COMPANY_RULE = 1
CATEGORY_RULE = 2

ANDROID = 0
IOS = 1

ANDROID_STR = 'android'
IOS_STR = 'ios'
#############CLASSIFIER NAMES###############
HEAD_CLASSIFIER = "Header Rule"
KV_CLASSIFIER = "KV Rule"
URI_CLASSIFIER = "URI Rule"
AGENT_CLASSIFIER = "Agent Rule"
CMAR_CLASSIFIER = 'CMAR Rule'
Query_BL_CLASSIFIER = 'Query_bl'
Agent_BL_CLASSIFIER = 'Agent_bl'

PATH_MINER = 'P'
KV_MINER = 'K'
#############EVALUATIONS###############
DISCOVERED_APP = 'discoveried_app'
DISCOVERED_APP_LIST = 'discoveried_app_list'
DETECTED_APP_LIST = 'detect_app_list'
PRECISION = 'precision'
RECALL = 'recall'
F1SCORE = 'f1'
RESULT = 'rst'
IDENTIFIER = '[IDENTIFIER]'
VERSION = '[VERSION]'
RANDOM = '[RANDOM]'
