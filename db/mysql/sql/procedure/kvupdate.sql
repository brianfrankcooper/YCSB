delimiter $$

CREATE PROCEDURE `kvupdate`(IN _key varchar(14), IN _field varchar(16), IN _value text)
BEGIN
  UPDATE record r JOIN fieldvalue v ON r.recordid = v.recordid
    SET v.fvalue = _value
    WHERE r.recordkey = _key AND r.recordfield = _field;
END$$

GRANT EXECUTE ON PROCEDURE `kvupdate` TO `mysql`$$