with staff_log as (
  select
      hsl.id,
      hsl.full_name,
      hsl.staff_id,
      hsl.change_type,
      row_number() over (partition by staff_id, hsl.effective_date order by hsl.id desc) as rank,
      hsl.email,
      hd.name as department,
      u3.full_name as team_lead,
      t.name as house,
      u2.full_name as manager,
      hsl.effective_date as start_date,
    from ergeon.hrm_stafflog hsl
      left join ergeon.hrm_team t on t.id = hsl.team_id
      left join ergeon.hrm_staff tl on tl.id = t.lead_id
      left join ergeon.core_user u3 on u3.id = tl.user_id
      left join ergeon.hrm_staff hs2 on hs2.id = hsl.manager_id
      left join ergeon.core_user u2 on u2.id = hs2.user_id
      left join ergeon.hrm_staffposition hsp on hsp.id = hsl.position_id
      left join ergeon.hrm_ladder hl on hl.id = hsp.ladder_id
      left join ergeon.hrm_department hd on hd.id = hl.department_id
    where
      hsl.created_at > '2018-04-15'
      and hsl.full_name <> 'Yannis Karamanlakis'
), staff_info as (
  select
    *,
    coalesce(lead(start_date) over (partition by staff_id order by start_date), current_date()) as end_date,
  from staff_log
  where team_lead is not null
  and rank = 1
)
select
  date(fr.posted_at, "America/Los_Angeles") as date,
  u.full_name,
  si.team_lead,
  si.house,
  u.email,
  fr.score,
  mc.label,
  ue.region,
  sr.region as region_gm,
  ue.segment,
from ergeon.feedback_review fr
  left join ergeon.marketing_localaccount la on la.id = fr.account_id
  left join ergeon.marketing_channel mc on mc.id = la.channel_id
  left join ergeon.hrm_staff h on h.id = fr.sales_staff_attributed_id
  left join ergeon.core_user u on u.id = h.user_id
  left join int_data.order_ue_materialized ue on ue.order_id = fr.order_id
  left join int_data.segment_region sr on sr.segment = ue.segment
  left join staff_info si on si.email = u.email and date(fr.posted_at, "America/Los_Angeles") >= si.start_date and date(fr.posted_at, "America/Los_Angeles") < si.end_date
where u.full_name is not null