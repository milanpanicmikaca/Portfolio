with ha_admin as (
    select
        datetime(c.created_at, "America/Los_Angeles") as datetime_lead,
        c.id,
        c.order_id,
        c.email,
        cast(json_extract_scalar(cv.event_object, '$.job_id') as int64) as sr_id,
        cast(json_extract_scalar(cv.event_object, '$.fee') as float64) as fee
    from ergeon.core_lead c
    left join ergeon.customers_visitoraction cv on cv.id = c.visitor_action_id
    where
        lower(json_extract_scalar(cv.event_object, '$.utm_source')) like '%home%advisor%'
        and
        -- to exclude HA Ads (this channel has job_id with letters and much more than 9)
        (length(json_extract_scalar(cv.event_object, '$.job_id')) <= 10
            or
            lower(json_extract_scalar(cv.event_object, '$.job_id')) is null) -- to include calls 
),

ha_unique as (
    -- need to use ha_spent_flat records if visitor action missing
    -- join by email/earliest lead whose date is before the order
    select
        ha.email,
        min(date) as min_date,
        avg(ha.fee) as avg_ha_fee
    from ext_marketing.ha_spend_flat ha
    group by 1
),

ha_spend_by_email as (
    select
        ha.*,
        m.old_region,
        ha1.srOid,
        ha1.sp_entity_id,
        case
            when ha1.email is null and length(ha.email) > 1 then (
                case when hat.sales_tax_rate > 0 then ha2.avg_ha_fee + hat.sales_tax_rate * ha2.avg_ha_fee
                    else ha2.avg_ha_fee end)
            when hat.sales_tax_rate > 0 then ha1.fee + hat.sales_tax_rate * ha1.fee
            else ha1.fee
        end as ha_fee
    from ha_admin ha
    left join ext_marketing.ha_spend_flat ha1 on ha1.srOid = ha.sr_id
    left join ha_unique ha2 on ha2.email = ha.email and ha2.min_date < ha.datetime_lead
    left join int_data.order_ue_materialized m on m.order_id = ha.order_id
    left join int_data.ha_taxed_states hat on hat.name = m.old_region
),

ha_requests as (
    select
        hsf.datetime_lead,
        hsf.sr_id,
        hsf.sp_entity_id,
        s.sp_company_name,
        case when r.lead_id is not null then 1 else 0 end as ha_refund_request,
        r.credit_request_type,
        hsf.ha_fee as lead_fee
    from ha_spend_by_email hsf
    left join googlesheets.ha_refund_requests r on r.lead_id = hsf.sr_id --requests to HA for refund
    left join googlesheets.ha_sp_entity s on s.sp_entity_id = hsf.sp_entity_id
    qualify row_number() over (partition by hsf.sr_id, r.credit_request_type order by hsf.datetime_lead) = 1
)

select
    hr.datetime_lead,
    hr.*except(datetime_lead, credit_request_type),
    case when hr.credit_request_type is null then "Not Requested" else hr.credit_request_type end as credit_request_type,
    case when hrf.sr_id is not null and hrf.credit_amount > 0 then 1 else 0 end as ha_refund,
    case when row_number() over (partition by hr.sr_id order by hr.credit_request_type) = 1 then 1 else 0 end as ha_lead,
    case
        when substr(sp_company_name, -1) = "1" then "driveway"
        when sp_company_name is not null then "fence"
    end as product,
    hrf.credit_amount as amount_refunded,
    hrf.credit_reason,
    datetime(hrf.created_at, "America/Los_Angeles") as datetime_refund,
    case when lower(hr.credit_request_type) like "%wrong task%" then 1 else 0 end as is_wrong_task,
    case when lower(hr.credit_request_type) like "%bogus contact%" then 1 else 0 end as is_bogus_contact,
    case when lower(hr.credit_request_type) like "%duplicate%" then 1 else 0 end as is_duplicate,
    case when lower(hr.credit_request_type) like "%unable to contact%" then 1 else 0 end as is_unable2contact,
    case when lower(hr.credit_request_type) like "%wrong zip%" then 1 else 0 end as is_wrong_zip,
    case when lower(hr.credit_request_type) like "%not willing%" then 1 else 0 end as is_cx_not_willing
from ha_requests hr
left join googlesheets.ha_refund hrf on hrf.sr_id = hr.sr_id
                                        and lower(replace(hrf.credit_reason, ' ', '')) = lower(replace(hr.credit_request_type, ' ', ''))
