with issue_data as
(
  select
    *,
    rank () over (partition by id order by timestamp desc) as rank
  from bigquerydatabase-270315.jira.issue_log
)
select
  * EXCEPT (rank)
from issue_data isd
where rank = 1;