MERGE `bigquerydatabase-270315.int_data_tests.logged_actions_storage` s
using (
  with
    rank_per_table_id
  as
  (
    select
    *,
    row_number() over (partition by table_name, table_id order by id desc) as rank_table_id
    from `bigquerydatabase-270315.int_data_tests.logged_actions_update` 
  )
  select * except(rank_table_id) from rank_per_table_id where rank_table_id = 1
) as u
on u.table_name = s.table_name and CAST(u.table_id as INT64) = s.table_id
when not matched and action in ("I", "U") then
INSERT (table_name, id, action_tstamp, new_data, table_id)
values (u.table_name, u.id, u.action_tstamp, u.new_data,CAST(u.table_id as INT64))
when matched and u.action = "D" then
DELETE
when matched and u.action = "U" 
  and s.action_tstamp < u.action_tstamp 
  and s.id < u.id then
UPDATE
SET s.id = u.id, s.action_tstamp = u.action_tstamp, s.new_data = u.new_data;