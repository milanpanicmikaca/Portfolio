DELETE
FROM `bigquerydatabase-270315.ergeon.logged_actions`
where concat(table_id,table_name) in
(
select 
concat(table_id,table_name) 
from ergeon.logged_actions 
where action = 'D' and table_id is not null
)