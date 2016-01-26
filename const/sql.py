#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import absolute_import, division, print_function, unicode_literals

from const import conf
#############SQLS###############
# SQL_SELECT_HTTP_PKGS = "select id, app, add_header, path, refer, hst, agent, dst, method,raw from %s where app =\'com.toysrus.tru\' "
# SQL_SELECT_HTTP_PKGS_LIMIT = "select id, app, add_header, path, refer, hst, agent, dst, raw from %s where limit %s"
# SQL_INSERT_HOST_RULES = 'INSERT INTO {} (label, support, confidence, host, rule_type) VALUES (%s, %s, %s, %s, %s)'.format(conf.ruleSet)
# SQL_DELETE_HOST_RULES = 'DELETE FROM {} WHERE paramkey IS NULL and pattens IS NULL and agent IS NULL and rule_type=%s'.format(conf.ruleSet)
# SQL_SELECT_HOST_RULES = 'SELECT host, label, rule_type, support FROM {} WHERE paramkey is NULL and pattens is NULL and agent IS NULL'.format(conf.ruleSet)
#
# SQL_INSERT_CMAR_RULES = 'INSERT INTO patterns (label, pattens, agent, confidence, support, host, rule_type) VALUES (%s, %s, %s, %s, %s, %s, %s)'
# SQL_DELETE_CMAR_RULES = 'DELETE FROM patterns WHERE pattens IS NOT NULL and rule_type = %s'
# SQL_SELECT_CMAR_RULES = 'SELECT label, pattens, agent, host, rule_type, support FROM patterns where paramkey is NULL'


SQL_SELECT_HTTP_PKGS = ('select id, app, add_header, path, refer, hst, agent, dst, method,raw'
                        'from %s '
                        "where method='GET' or method='POST'")
SQL_SELECT_HTTP_PKGS_LIMIT = ("select id, app, add_header, path, refer, hst, agent, dst, method,raw"
                              "from %s"
                              "where method='GET' or method='POST' limit %s")
############################
SQL_CLEAN_ALL_RULES = 'DELETE FROM {}'.format(conf.ruleSet)
############################
SQL_DELETE_KV_RULES = ('DELETE FROM {}'
                       'WHERE paramkey IS NOT NULL and rule_type=%s').format(conf.ruleSet)

SQL_INSERT_KV_RULES = ('INSERT INTO {} (label, support, confidence, host, paramkey, paramvalue, rule_type)'
                       'VALUES (%s, %s, %s, %s, %s, %s, %s)').format(conf.ruleSet)

SQL_SELECT_KV_RULES = ('SELECT paramkey, paramvalue, host, label, confidence, rule_type, support'
                       'FROM {}'
                       'WHERE paramkey IS NOT NULL').format(conf.ruleSet)
############################
SQL_INSERT_AGENT_RULES = ('INSERT INTO {} (host, prefix, identifier, suffix, label, support, confidence, rule_type, label_type)'
                          'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)').format(conf.ruleSet)

SQL_DELETE_AGENT_RULES = ('DELETE FROM {}'
                          'WHERE agent IS NOT NULL and rule_type=%s').format(conf.ruleSet)

SQL_SELECT_AGENT_RULES = ('SELECT host, prefix, identifier, suffix, label, support, confidence, rule_type, label_type\n'
                          'FROM {}'
                          'WHERE rule_type = 3').format(conf.ruleSet)
############################
SQL_UPDATE_PKG = ('UPDATE %s SET classified = %s'
                  'WHERE id = %s')
############################
SQL_CREATE_PATTERN = ('CREATE TABLE `{}` ('
                      '  `id` INT(11) UNSIGNED NOT NULL AUTO_INCREMENT,'
                      '  `label` VARCHAR(100) DEFAULT NULL,'
                      '  `pattens` VARCHAR(2000) DEFAULT NULL,'
                      '  `host` VARCHAR(100) DEFAULT NULL,'
                      '  `rule_type` INT(11) DEFAULT NULL,'
                      '  `paramkey` VARCHAR(5000) DEFAULT NULL,'
                      '  `paramvalue` VARCHAR(5000) DEFAULT NULL,'
                      '  `support` INT(11) DEFAULT NULL,'
                      '  `confidence` DOUBLE DEFAULT NULL,'
                      '  `agent` VARCHAR(2000) DEFAULT NULL,'
                      '  `prefix` VARCHAR(2000) DEFAULT NULL,'
                      '  `identifier` VARCHAR(2000) DEFAULT NULL,'
                      '  `suffix` VARCHAR(2000) DEFAULT NULL,'
                      '  `label_type` INT(11) DEFAULT NULL,'
                      '  PRIMARY KEY (`id`)'
                      ') ENGINE=InnoDB AUTO_INCREMENT=19671453 DEFAULT CHARSET=latin1;').format(conf.ruleSet)
############################
SQL_CREATE_PACKAGE= ('CREATE TABLE `%s` ('
                     '  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,'
                     '  `app` varchar(100) NOT NULL DEFAULT \'\','
                     '  `add_header` varchar(1000) DEFAULT NULL,'
                     '  `refer` varchar(500) DEFAULT NULL,'
                     '  `src` varchar(15) NOT NULL DEFAULT \'\','
                     '  `dst` varchar(15) NOT NULL DEFAULT \'\','
                     '  `path` varchar(2000) DEFAULT NULL,'
                     '  `time` double NOT NULL,'
                     '  `hst` varchar(100) DEFAULT NULL,'
                     '  `accpt` varchar(500) DEFAULT NULL,'
                     '  `agent` varchar(500) DEFAULT NULL,'
                     '  `author` varchar(100) DEFAULT NULL,'
                     '  `cntlength` int(11) DEFAULT NULL,'
                     '  `cnttype` varchar(100) DEFAULT NULL,'
                     '  `method` varchar(20) DEFAULT NULL,'
                     '  `size` int(11) DEFAULT NULL,'
                     '  `httptype` int(1) DEFAULT NULL,'
                     '  `name` varchar(100) DEFAULT NULL,'
                     '  `category` varchar(100) DEFAULT NULL,'
                     '  `company` varchar(200) DEFAULT NULL,'
                     '  `classified` int(11) DEFAULT NULL,'
                     '  `raw` varchar(3000) DEFAULT NULL,'
                     '  PRIMARY KEY (`id`)'
                     ') ENGINE=InnoDB AUTO_INCREMENT=303489 DEFAULT CHARSET=utf8;')