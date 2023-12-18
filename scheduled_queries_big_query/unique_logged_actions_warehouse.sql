select 
  *
from warehouse.logged_actions
qualify row_number() over (partition by table_name,JSON_EXTRACT_SCALAR(new_data, '$.id') order by action_tstamp desc) = 1