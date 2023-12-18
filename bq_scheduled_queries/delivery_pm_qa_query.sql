with scores_old as (
    select
        *
    from int_data.pm_qa
    unpivot
    (
        score for code in (
            del_pm_int_01,
            del_pm_int_02,
            del_pm_int_03,
            del_pm_int_04,
            del_pm_int_05,
            del_pm_int_06,
            del_pm_int_07,
            del_pm_int_08,
            del_pm_int_09,
            del_pm_fol1_01,
            del_pm_fol1_02,
            del_pm_fol1_03,
            del_pm_fol1_04,
            del_pm_fol1_05,
            del_pm_fol1_06,
            del_pm_fol1_07,
            del_pm_fol2_01,
            del_pm_fol2_02,
            del_pm_fol2_03,
            del_pm_fol2_04,
            del_pm_fol2_05,
            del_pm_fol2_06,
            del_pm_fol3_01,
            del_pm_fol3_02,
            del_pm_fol3_03,
            del_pm_fol3_04,
            del_pm_fol3_05,
            del_pm_fol3_06,
            del_pm_fol3_07,
            del_pm_fol3_08,
            del_pm_fol4_01,
            del_pm_fol4_02,
            del_pm_fol4_03,
            del_pm_fol4_04,
            del_pm_fol4_05,
            del_pm_fol4_06,
            del_pm_fol4_07,
            del_pm_fc1_01,
            del_pm_fc1_02,
            del_pm_fc1_03,
            del_pm_fc1_04,
            del_pm_fc1_05,
            del_pm_fc1_06,
            del_pm_fc1_07,
            del_pm_fc1_08,
            del_pm_fc1_09,
            del_pm_fc1_10,
            del_pm_fc1_11,
            del_pm_fc1_12,
            del_pm_fc1_13,
            del_pm_fc1_14,
            del_pm_fc1_15,
            del_pm_fc1_16
        )
    )
),

translation_old as (
    select * from int_data.pm_qa_translation
),

pm_qa_old as (
    select
        qa.full_name as qa_name,
        datetime(cast(s.timestamp as timestamp), 'America/Los_Angeles') as time_stamp,
        s.email_address,
        s.pd_deal_key,
        s.admin_link,
        s.pm_name as project_manager,
        cast(null as string) as project_Type,
        cast(null as string) as region,
        cast(null as string) as pm_interaction_Call,
        s.score,
        s.code,
        tl.house as house,
        t.question,
        case
            when question like 'Intro Call %' then 'Intro Call'
            when question like 'Follow up call 1 %' then 'Follow Up Call 1'
            when question like 'Follow up call 2 %' then 'Follow Up Call 2'
            when question like 'Follow up call 3 %' then 'Follow Up Call 3'
            when question like 'Follow up call 4 %' then 'Follow Up Call 4'
            when question like 'Feedback Call QA %' then 'Feedback Call QA'
        end as category
    from scores_old s
    join int_data.delivery_team_lead tl on tl.full_name = s.pm_name
    left join translation_old t on lower(t.code) = lower(s.code)
    left join int_data.hr_dashboard qa on qa.email = s.email_address
    left join int_data.pm_qa q on q.timestamp = s.timestamp and q.email_address = s.email_address
    where s.pm_name != 'TEST'
        and s.timestamp < '2022-06-09' --for 2022-06-09 QA exists in new table
),

scores_new as (
    select
        *
    from int_data.pm_qa_2022
    unpivot
    (
        score for code in (
            del_pm_email_01,
            del_pm_email_02,
            del_pm_int_01,
            del_pm_int_02,
            del_pm_int_03,
            del_pm_int_04,
            del_pm_int_05,
            del_pm_int_06,
            del_pm_int_07,
            del_pm_int_08,
            del_pm_int_10,
            del_pm_int_11,
            del_pm_int_09,
            del_pm_fol1_01,
            del_pm_fol1_02,
            del_pm_fol1_03,
            del_pm_fol1_04,
            del_pm_fol1_05,
            del_pm_fol1_06,
            del_pm_fol1_08,
            del_pm_fol1_09,
            del_pm_fol1_07,
            del_pm_fol2_01,
            del_pm_fol2_02,
            del_pm_fol2_03,
            del_pm_fol2_04,
            del_pm_fol2_05,
            del_pm_fol2_07,
            del_pm_fol2_06,
            del_pm_fol3_01,
            del_pm_fol3_02,
            del_pm_fol3_03,
            del_pm_fol3_04,
            del_pm_fol3_05,
            del_pm_fol3_06,
            del_pm_fol3_07,
            del_pm_fol3_09,
            del_pm_fol3_08,
            del_pm_fol4_01,
            del_pm_fol4_02,
            del_pm_fol4_03,
            del_pm_fol4_04,
            del_pm_fol4_05,
            del_pm_fol4_06,
            del_pm_fol4_08,
            del_pm_fol4_09,
            del_pm_fol4_07,
            del_pm_fc1_02,
            del_pm_fc1_03,
            del_pm_fc1_04,
            del_pm_fc1_05,
            del_pm_fc1_07,
            del_pm_fc1_08,
            del_pm_fc1_09,
            del_pm_fc1_10,
            del_pm_fc1_11,
            del_pm_fc1_12,
            del_pm_fc1_14,
            del_pm_fc1_15,
            del_pm_fc1_17,
            del_pm_fc1_16,
            del_pm_rev_01,
            del_pm_rev_02,
            del_pm_rev_03,
            del_pm_rev_04,
            del_pm_rev_05,
            del_pm_rev_06,
            del_pm_rev_07
        )
    )
),

translation_new as (
    select * from int_data.pm_qa_2022_translation
),

pm_qa_new as (
    select
        qa.full_name as qa_name,
        datetime(cast(s.time_stamp as timestamp), 'America/Los_Angeles') as time_stamp,
        s.email_address,
        s.pd_deal_key,
        s.admin_link,
        s.pm_name as project_manager,
        cast(null as string) as project_Type,
        cast(null as string) as region,
        cast(null as string) as pm_interaction_Call,
        case when lower(question) like 'review request%' and lower(score) = 'no' then "0"
                                                                   when lower(question) like 'review request%' and lower(score) = 'yes' then "2"
                                                                   else score
        end as score,
        s.code,
        tl.house as house,
        t.question,
        case
            when question like 'Handover%' then 'Handover Email'
            when question like 'Intro Call %' then 'Intro Call'
            when question like 'Follow up call 1 %' then 'Follow Up Call 1'
            when question like 'Follow up call 2 %' then 'Follow Up Call 2'
            when question like 'Follow up call 3 %' then 'Follow Up Call 3'
            when question like 'Follow up call 4 %' then 'Follow Up Call 4'
            when question like 'Feedback Call QA %' then 'Feedback Call QA'
            when question like 'Review Request%' then 'Review Request'
        end as category
    from scores_new s
    join int_data.delivery_team_lead tl on tl.full_name = s.pm_name
    left join translation_new t on lower(t.code) = lower(s.code)
    left join int_data.hr_dashboard qa on qa.email = s.email_address
    left join int_data.pm_qa_2022 q on q.time_stamp = s.time_stamp and q.email_address = s.email_address
    where s.pm_name != 'TEST'
)

select * from pm_qa_old
--where not regexp_contains(score, r'[a-zA-Z\s]+') --added 14/9/2022
union all
select * from pm_qa_new
--where not regexp_contains(score, r'[a-zA-Z\s]+') -- added 14/9/2022
