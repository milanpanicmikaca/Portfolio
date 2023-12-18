with scores_ix as (
select 
  date(ix.created_at, "America/Los_Angeles") as date,
  workload_score,
  payment_score,
  communication_score,
  trust_score,
  future_score,
  (workload_score + payment_score + communication_score + trust_score + future_score)/5 as score,
  tt.subject as task,
  u.full_name,
  u2.full_name as created_by,
  rank() over (partition by u.full_name order by ix.created_at desc) as rank_score
from ergeon.feedback_installersatisfaction ix
left join ergeon.tasks_task tt on tt.id = ix.task_id
left join ergeon.contractor_contractor c on c.id = ix.contractor_id
left join ergeon.contractor_contractorcontact cc on cc.id = c.contact_id
left join ergeon.core_user u on u.id = cc.user_id
left join ergeon.core_user u2 on u2.id = ix.created_by_id
)
select 
* 
from scores_ix 
where rank_score = 1 