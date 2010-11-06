delimiter $$

CREATE PROCEDURE `kvinsert`(IN _key varchar(14), IN _field varchar(16), IN _value text)
BEGIN
  INSERT INTO record (recordkey, recordfield) VALUES (_key, _field);
  INSERT INTO fieldvalue (recordid, fvalue) VALUES (last_insert_id(), _value);
END$$

GRANT EXECUTE ON PROCEDURE `kvinsert` TO `mysql`$$
