from collections import namedtuple

Prediction = namedtuple('Prediction', 'label, score, evidence')
Rule = namedtuple('Rule', 'secdomain, key, score, labelNum')
NULLPrediction = Prediction(None, -1, None)

LABEL = 'l'
ERROR = 'e'
SCORE = 's'
SUPPORT = 't'
REGEX_OBJ = 'r'

#############TYPE###############
APP_RULE = 0
COMPANY_RULE = 1
CATEGORY_RULE = 2

ANDROID = 0
IOS = 1

ANDROID_STR = 'android'
IOS_STR = 'ios'
#############SQLS###############
SQL_SELECT_HTTP_PKGS = "select id, app, add_header, path, refer, hst, agent, dst, method,raw from %s where method=\'GET\' or method=\'POST\'"
SQL_SELECT_HTTP_PKGS_LIMIT = "select id, app, add_header, path, refer, hst, agent, dst, method,raw from %s where method=\'GET\' or method=\'POST\'limit %s"
#SQL_SELECT_HTTP_PKGS = "select id, app, add_header, path, refer, hst, agent, dst, method,raw from %s where app =\'com.toysrus.tru\' "
# SQL_SELECT_HTTP_PKGS_LIMIT = "select id, app, add_header, path, refer, hst, agent, dst, raw from %s where limit %s" 
SQL_CLEAN_ALL_RULES = 'DELETE FROM patterns'
SQL_INSERT_HOST_RULES = 'INSERT INTO patterns (label, support, confidence, host, rule_type) VALUES (%s, %s, %s, %s, %s)'
SQL_DELETE_HOST_RULES = 'DELETE FROM patterns WHERE paramkey IS NULL and pattens IS NULL and agent IS NULL and rule_type=%s'
SQL_SELECT_HOST_RULES = 'SELECT host, label, rule_type, support FROM patterns WHERE paramkey is NULL and pattens is NULL and agent IS NULL'

SQL_INSERT_CMAR_RULES = 'INSERT INTO patterns (label, pattens, agent, confidence, support, host, rule_type) VALUES (%s, %s, %s, %s, %s, %s, %s)'
SQL_DELETE_CMAR_RULES = 'DELETE FROM patterns WHERE pattens IS NOT NULL and rule_type = %s'
SQL_SELECT_CMAR_RULES = 'SELECT label, pattens, agent, host, rule_type, support FROM patterns where paramkey is NULL'

SQL_DELETE_KV_RULES = 'DELETE FROM patterns WHERE paramkey IS NOT NULL and rule_type=%s'
SQL_INSERT_KV_RULES = 'INSERT INTO patterns (label, support, confidence, host, paramkey, paramvalue, rule_type) VALUES (%s, %s, %s, %s, %s, %s, %s)'
SQL_SELECT_KV_RULES = 'SELECT paramkey, paramvalue, host, label, confidence, rule_type, support FROM patterns WHERE paramkey IS NOT NULL'

SQL_INSERT_AGENT_RULES = 'INSERT INTO patterns (label, support, confidence, agent, host, rule_type) VALUES (%s, %s, %s, %s, %s, %s)'
SQL_DELETE_AGENT_RULES = 'DELETE FROM patterns WHERE agent IS NOT NULL and rule_type=%s'
SQL_SELECT_AGENT_RULES = 'SELECT host, agent , label, rule_type FROM patterns WHERE agent IS NOT NULL and host is NULL'

SQL_UPDATE_PKG = "UPDATE %s SET classified = %s WHERE id = %s"
#############CLASSIFIER NAMES###############
HEAD_CLASSIFIER = "Header Rule"
KV_CLASSIFIER = "KV Rule"
URI_CLASSIFIER = "URI Rule"
AGENT_CLASSIFIER = "Agent Rule"
CMAR_CLASSIFIER = 'CMAR Rule'

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



