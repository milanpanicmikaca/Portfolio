select 
  datetime(ym.created_at, "America/Los_Angeles") as date, 
  u.full_name,
  h.ladder_name,
  1 as messages,
  datetime_diff(ym.replied_at, ym.created_at, minute) as response_min,
  case when datetime_diff(ym.replied_at, ym.created_at, minute) <= 10 then 1 else 0 end as in_sla,
  case when datetime_diff(ym.replied_at, ym.created_at, minute) > 10 then 1 else 0 end as non_sla,
from ergeon.marketing_yelpmessage ym
left join ergeon.marketing_localaccount ac on ac.id = ym.local_account_id
left join ergeon.core_user u on u.id = ym.modified_by_id
left join int_data.hr_dashboard h on h.email = u.email
where datetime_diff(ym.replied_at, ym.created_at, minute) >= 0
and u.full_name is not null