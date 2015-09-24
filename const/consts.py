from collections import namedtuple

Prediction = namedtuple('Prediction', 'label, score, evidence')
NULLPrediction = Prediction(None, -1, None)


LABEL = 'l'
SCORE = 's'
SUPPORT = 't'
REGEX_OBJ = 'r'

#############TYPE###############
APP_RULE = 1
COMPANY_RULE = 2
CATEGORY_RULE = 3

ANDROID = 0
IOS = 1

ANDROID_STR = 'android'
IOS_STR = 'ios'
#############SQLS###############
SQL_SELECT_HTTP_PKGS = "select id, app, add_header, path, refer, hst, agent, dst, raw from %s where method=\'GET\' or method=\'POST\'" 
SQL_SELECT_HTTP_PKGS_LIMIT = "select id, app, add_header, path, refer, hst, agent, dst, raw from %s where method=\'GET\' or method=\'POST\'limit %s" 
# SQL_SELECT_HTTP_PKGS = "select id, app, add_header, path, refer, hst, agent, dst, raw from %s" 
# SQL_SELECT_HTTP_PKGS_LIMIT = "select id, app, add_header, path, refer, hst, agent, dst, raw from %s where limit %s" 
SQL_CLEAN_ALL_RULES = 'DELETE FROM patterns'
SQL_INSERT_HOST_RULES = 'INSERT INTO patterns (label, support, confidence, host, rule_type) VALUES (%s, %s, %s, %s, %s)'
SQL_DELETE_HOST_RULES = 'DELETE FROM patterns WHERE paramkey IS NULL and pattens IS NULL and agent IS NULL and rule_type=%s'
SQL_SELECT_HOST_RULES = 'SELECT host, label, rule_type, support FROM patterns WHERE paramkey is NULL and pattens is NULL and agent IS NULL'

SQL_INSERT_CMAR_RULES = 'INSERT INTO patterns (label, pattens, confidence, support, host, rule_type) VALUES (%s, %s, %s, %s, %s, %s)'
SQL_DELETE_CMAR_RULES ='DELETE FROM patterns WHERE pattens IS NOT NULL and rule_type = %s'
SQL_SELECT_CMAR_RULES = 'SELECT label, pattens, host, rule_type, support FROM patterns where pattens is not NULL'

SQL_DELETE_KV_RULES = 'DELETE FROM patterns WHERE paramkey IS NOT NULL and rule_type=%s'
SQL_INSERT_KV_RULES = 'INSERT INTO patterns (label, support, confidence, host, paramkey, paramvalue, rule_type) VALUES (%s, %s, %s, %s, %s, %s, %s)'
SQL_SELECT_KV_RULES = 'SELECT paramkey, paramvalue, host, label, confidence, rule_type, support FROM patterns WHERE paramkey IS NOT NULL' 

SQL_INSERT_AGENT_RULES = 'INSERT INTO patterns (label, support, confidence, agent, rule_type) VALUES (%s, %s, %s, %s, %s)'
SQL_DELETE_AGENT_RULES = 'DELETE FROM patterns WHERE agent IS NOT NULL and rule_type=%s'
SQL_SELECT_AGENT_RULES = 'SELECT agent , label, rule_type FROM patterns WHERE agent IS NOT NULL'
#############CLASSIFIER NAMES###############
HEAD_CLASSIFIER = "Header Rule"
KV_CLASSIFIER = "KV Rule"
CMAR_CLASSIFIER = "CMAR Rule"
HOST_CLASSIFIER = "Host Rule"
AGENT_CLASSIFIER = "Agent Rule"

#############EVALUATIONS###############
DISCOVERED_APP ='discoveried_app' 
PRECISION = 'precision'
RECALL = 'recall'
F1SCORE = 'f1'
