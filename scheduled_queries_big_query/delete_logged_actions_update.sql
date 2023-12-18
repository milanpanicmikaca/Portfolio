DELETE
from bigquerydatabase-270315.int_data_tests.logged_actions_update u
where concat(u.table_name,u.table_id) in
(
select
  concat(s.table_name,s.table_id)
from bigquerydatabase-270315.int_data_tests.logged_actions_update u
left join int_data_tests.logged_actions_storage s on cast(u.table_id as INT64) = s.table_id and u.table_name = s.table_name
where u.id <= s.id and u.action_tstamp <= s.action_tstamp
);

DELETE
from int_data_tests.logged_actions_update u
where u.id in
(
select
  u.id
from int_data_tests.logged_actions_update u
left join int_data_tests.logged_actions_storage s on cast(u.table_id as INT64) = s.table_id and s.table_name = u.table_name
where action = 'D' and s.table_id is null
);