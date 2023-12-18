with scores as (
  select 
    *
  from int_data.fam_qa q 
  unpivot 
  (score for code 
    in (comm_01, comm_02, comm_03, comm_04, comm_05, comm_feedback, cx_eng_01, cx_eng_02, cx_eng_03, cx_eng_04, cx_eng_05, cx_eng_06, cx_eng_07,      cx_eng_feedback, sal_01, sal_02, sal_feedback, compl_01, compl_02, compl_03, compl_feedback))
), 
translation as (
  select * from int_data.fam_qa_translation qt
)
select 
    qa.full_name as qa_name,
    s.*,
    t.question,
    case 
        when s.code like 'comm%' then 'Communication'
        when s.code like 'cx_eng%' then 'Customer Engagement'
        when s.code like 'sal%' then 'Salesmanship'
        when s.code like 'compl%' then 'Compliance' 
    end as category,
from scores s
left join translation t on lower(t.code) = lower(s.code)
left join int_data.hr_dashboard qa on qa.email = s.email_address