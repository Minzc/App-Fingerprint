#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import absolute_import, division, print_function, unicode_literals

import json

with open("resource/config.json") as f:
    j = json.load(f)

    query_scoreT = j["query"]["score"]
    query_labelT = j["query"]["label"]

    assert type(query_labelT) == float

    path_scoreT = j["path"]["score"]
    path_labelT = j["path"]["label"]

    agent_support = j["agent"]["support"]

    sample_rate = j["sample"]

    package_limit = j["package.limit"] if j["package.limit"] != 0 else None

    TestBaseLine = j["baseline"]

    INSERT = j["insert"]
    assert type(INSERT) == bool

    ruleSet = j["ruleset"]

    debug = j["debug"]

    ensamble = j["ensamble"]

    if j["mode"] == "l":
        db_user = j["db"]["local"]["username"]
        db_pwd = j["db"]["local"]["password"]
    else:
        db_user = j["db"]["server"]["username"]
        db_pwd = j["db"]["server"]["password"]

    mode = j["mode"]

    region = j["region"]