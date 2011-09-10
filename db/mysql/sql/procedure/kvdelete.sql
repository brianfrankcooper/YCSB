delimiter $$

CREATE PROCEDURE `kvdelete`(IN _key varchar(14))
BEGIN
  DELETE r, v FROM record r, fieldvalue v WHERE r.recordid = v.recordid AND r.recordkey = _key;
END$$

GRANT EXECUTE ON PROCEDURE `kvdelete` TO `mysql`$$
