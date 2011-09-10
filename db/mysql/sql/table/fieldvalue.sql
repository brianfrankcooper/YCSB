delimiter $$

CREATE TABLE `fieldvalue` (
  `recordid` bigint NOT NULL,
  `fvalue` text,
  PRIMARY KEY (`recordid`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8$$

GRANT INSERT,SELECT,UPDATE,DELETE ON TABLE fieldvalue TO mysql$$
GRANT
  INSERT(recordid,fvalue),
  SELECT(recordid,fvalue),
  UPDATE(recordid,fvalue),
  DELETE
ON TABLE fieldvalue TO mysql$$
