delimiter $$

CREATE TABLE `fieldvalue` (
  `recordid` bigint NOT NULL,
  `fvalue` text,
  PRIMARY KEY (`recordid`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8$$

grant insert,select,update,delete on table fieldvalue to mysql$$
grant
  insert(recordid,fvalue),
  select(recordid,fvalue),
  update(recordid,fvalue),
  delete
on table fieldvalue to mysql$$