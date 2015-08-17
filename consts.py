from collections import namedtuple

Prediction = namedtuple('Prediction', 'label, score, evidence')

APP_RULE = 1
COMPANY_RULE = 2
CATEGORY_RULE = 3

ANDROID = 0
IOS = 1

LABEL = 'l'
SCORE = 's'
SUPPORT = 't'
