#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import absolute_import, division, print_function, unicode_literals

import json

with open("resource/config.json") as f:
    j = json.load(f)

    query_scoreT = j["query"]["score"]
    query_labelT = j["query"]["label"]
    query_K = j["query"]["K"]

    assert type(query_labelT) == float

    path_scoreT = j["path"]["score"]
    path_labelT = j["path"]["label"]
    path_K = j["path"]["K"]

    head_scoreT = j["head"]["score"]
    head_labelT = j["head"]["label"]
    head_K = j["head"]["K"]

    agent_support = j["agent"]["support"]
    agent_score = j["agent"]["score"]
    agent_K = j["agent"]["K"]

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