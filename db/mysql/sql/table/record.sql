delimiter $$

CREATE TABLE `record` (
  `recordid` bigint NOT NULL AUTO_INCREMENT,
  `recordkey` varchar(14) NOT NULL,
  `recordfield` varchar(16) NOT NULL,
  PRIMARY KEY (`recordid`),
  UNIQUE KEY (`recordkey`, `recordfield`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8 PACK_KEYS=1 ROW_FORMAT=FIXED$$

GRANT INSERT,SELECT,UPDATE,DELETE ON TABLE record TO mysql$$
GRANT
  INSERT(recordid,recordkey,recordfield),
  SELECT(recordid,recordkey,recordfield),
  UPDATE(recordid,recordkey,recordfield),
  DELETE
ON TABLE record TO mysql$$
