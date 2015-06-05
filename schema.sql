CREATE TABLE `patterns` (
 `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
 `label` varchar(100) DEFAULT NULL,
 `pattens` varchar(2000) DEFAULT NULL,
 `support` int(11) DEFAULT NULL,
 `confidence` double DEFAULT NULL,
 `host` varchar(100) DEFAULT NULL,
 `kvpattern` varchar(3000) DEFAULT NULL,
 `rule_type` int(11) DEFAULT NULL,
 PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=17595 DEFAULT CHARSET=latin1;

CREATE TABLE `packages` (
 `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
 `app` varchar(100) NOT NULL DEFAULT '',
 `add_header` varchar(1000) DEFAULT NULL,
 `refer` varchar(500) DEFAULT NULL,
 `src` varchar(15) NOT NULL DEFAULT '',
 `dst` varchar(15) NOT NULL DEFAULT '',
 `path` varchar(2000) DEFAULT NULL,
 `time` double NOT NULL,
 `hst` varchar(100) DEFAULT NULL,
 `accpt` varchar(500) DEFAULT NULL,
 `agent` varchar(500) DEFAULT NULL,
 `author` varchar(100) DEFAULT NULL,
 `cntlength` int(11) DEFAULT NULL,
 `cnttype` varchar(100) DEFAULT NULL,
 `method` varchar(20) DEFAULT NULL,
 `size` int(11) DEFAULT NULL,
 `httptype` int(1) DEFAULT NULL,
 `name` varchar(100) DEFAULT NULL,
 `category` varchar(100) DEFAULT NULL,
 `company` varchar(200) DEFAULT NULL,
 `classified` int(11) DEFAULT NULL,
 `raw` varchar(3000) DEFAULT NULL,
 PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=158263 DEFAULT CHARSET=utf8;
