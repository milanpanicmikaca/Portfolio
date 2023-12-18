-- Only deletes 'D' actions that have been processed
  FOR TABLE IN (
  SELECT
    DISTINCT table_name
  FROM
    ergeon_replication.logged_actions_update
  WHERE
    action = 'D') DO
EXECUTE IMMEDIATE
  CONCAT("DELETE FROM ergeon_replication.logged_actions_update where id in (SELECT u.id FROM ergeon_replication.logged_actions_update u left join ergeon_storage.", TABLE.table_name,"_storage s on u.table_id = JSON_EXTRACT_SCALAR(s.json, '$.id') where s.id is null and u.action = 'D' and u.table_name = '", TABLE.table_name, "')");
END
  FOR;