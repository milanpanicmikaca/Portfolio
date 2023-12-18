select * except (rank)
from
(
  select
    row_number() over (partition by table_name, table_id order by id desc) as rank,
    *
  from ergeon.logged_actions la
)
where
  rank = 1