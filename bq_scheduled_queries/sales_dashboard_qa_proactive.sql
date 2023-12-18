with scores as (
    select 
    *
FROM int_data.sales_qa sq 
UNPIVOT 
(score FOR code 
IN (SAL_CS_QUAL_01, SAL_CS_QUAL_02, SAL_CS_QUAL_03, SAL_CS_QUAL_04, SAL_CS_QUAL_05, SAL_CS_QUAL_06, SAL_CS_QUAL_07, SAL_CS_QUAL_08, SAL_CS_QUAL_09, SAL_CS_QUAL_10, 
    SAL_CS_QUAL_11, SAL_CS_QUAL_12, SAL_CS_QUAL_13, SAL_CS_QUAL_14, SAL_CS_QUAL_15, SAL_CS_QUAL_16, SAL_CS_QUAL_17, SAL_CS_QUAL_18, SAL_CS_QUAL_19, SAL_CS_QUAL_20, 
    SAL_CS_QUAL_21, SAL_CS_QUAL_22, SAL_CS_QUAL_23, SAL_CS_ONSITE_01, SAL_CS_ONSITE_02, SAL_CS_ONSITE_03, SAL_CS_ONSITE_04, SAL_CS_ONSITE_05, SAL_CS_ONSITE_06, 
    SAL_CS_ONSITE_07, SAL_CS_ONSITE_08, SAL_CS_F_ONSITE_09, SAL_CS_F_QR_01, SAL_CS_F_QR_02, SAL_CS_F_QR_03, SAL_CS_F_QR_04, SAL_CS_F_QR_05, SAL_CS_F_QR_06, SAL_CS_F_QR_07, 
    SAL_CS_F_QR_08, SAL_CS_F_QR_09, SAL_CS_F_QR_10, SAL_CS_F_QR_11, SAL_CS_F_QR_12, SAL_CS_F_QR_13, SAL_CS_F_QR_14, SAL_CS_F_QR_15, SAL_CS_F_QR_16, SAL_CS_F_QR_17, 
    SAL_CS_F_CLOSE_01, SAL_CS_F_CLOSE_02, SAL_CS_F_CLOSE_03, SAL_CS_F_CLOSE_04, SAL_CS_F_CLOSE_05, SAL_CS_F_CLOSE_06, SAL_CS_F_CLOSE_07, SAL_CS_F_CLOSE_08, SAL_CS_F_CLOSE_09, 
    SAL_CS_F_QUAL_01, SAL_CS_H_QUAL_01, SAL_CS_H_QUAL_02, SAL_CS_H_QUAL_03, SAL_CS_H_QUAL_04, SAL_CS_H_QUAL_05, SAL_CS_H_QUAL_06, SAL_CS_H_QUAL_07, SAL_CS_H_QUAL_08, 
    SAL_CS_H_QUAL_09, SAL_CS_H_QUAL_10, SAL_CS_H_QUAL_11, SAL_CS_H_QUAL_12, SAL_CS_H_QUAL_13, SAL_CS_H_QUAL_14, SAL_CS_H_QUAL_15, SAL_CS_H_QUAL_16, SAL_CS_H_QUAL_17, 
    SAL_CS_H_QUAL_18, SAL_CS_H_QUAL_19, SAL_CS_H_QUAL_20, SAL_CS_H_ONSITE_01, SAL_CS_H_ONSITE_02, SAL_CS_H_ONSITE_03, SAL_CS_H_ONSITE_04, SAL_CS_H_ONSITE_05, SAL_CS_H_ONSITE_06, 
    SAL_CS_H_ONSITE_07, SAL_CS_H_ONSITE_08, SAL_CS_H_ONSITE_09, SAL_CS_H_QR_01, SAL_CS_H_QR_02, SAL_CS_H_QR_03, SAL_CS_H_QR_04, SAL_CS_H_QR_05, SAL_CS_H_QR_06, SAL_CS_H_QR_07, 
    SAL_CS_H_QR_08, SAL_CS_H_QR_09, SAL_CS_H_QR_10, SAL_CS_H_QR_11, SAL_CS_H_QR_12, SAL_CS_H_QR_13, SAL_CS_H_QR_14, SAL_CS_H_QR_15, SAL_CS_H_QR_16, SAL_CS_H_QR_17, 
    SAL_CS_H_CLOSE_01, SAL_CS_H_CLOSE_02, SAL_CS_H_CLOSE_03, SAL_CS_H_CLOSE_04, SAL_CS_H_CLOSE_05, SAL_CS_H_CLOSE_06, SAL_CS_H_CLOSE_07, SAL_CS_H_CLOSE_08, SAL_CS_H_CLOSE_09))
), translation as (
    select * from int_data.sales_qa_translation sqt
)
select 
    qa.full_name as qa_name,
    s.*,
    db.house as current_house,
	cast(db.tenure as int64) as tenure,
    t.question,
    case 
        when question like 'Qualification Call %' then 'Qualification Call'
        when question like 'On-Site %' then 'On-Site'
        when question like 'Quote Review %'then 'Quote Review' 
        when question like 'Close Call %'then 'Close Call'
        when question like 'Qual-Call %' then 'Qualification Call'
        when question like 'Close Call %'then 'Close Call'
    end as category,
from scores s
left join translation t on lower(t.code) = lower(s.code)
left join int_data.hr_dashboard hd on hd.full_name = s.cs_name
left join int_data.hr_dashboard qa on qa.email = s.email_address
left join int_data.employee_db db on db.email = hd.email
left join int_data.sales_qa sq on sq.timestamp = s.timestamp and sq.email_address = s.email_address