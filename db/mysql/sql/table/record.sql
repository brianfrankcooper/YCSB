delimiter $$

CREATE TABLE `record` (
  `recordid` bigint NOT NULL AUTO_INCREMENT,
  `recordkey` varchar(14) NOT NULL,
  `recordfield` varchar(16) NOT NULL,
  PRIMARY KEY (`recordid`),
  UNIQUE KEY (`recordkey`, `recordfield`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8 PACK_KEYS=1 ROW_FORMAT=FIXED$$

grant insert,select,update,delete on table record to mysql$$
grant
  insert(recordid,recordkey,recordfield),
  select(recordid,recordkey,recordfield),
  update(recordid,recordkey,recordfield),
  delete
on table record to mysql$$
