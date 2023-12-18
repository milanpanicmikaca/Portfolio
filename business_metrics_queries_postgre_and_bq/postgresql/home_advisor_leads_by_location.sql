-- upload to BQ
with
calc_leads_detailed as
(
        select
                   l.id,
                   l.created_at,
                   case 
                           when cv.event_object ->> 'utm_source' = 'homeadvisor' and cv.event_object ->> 'utm_location' = 'Sacramento' then 'is_sacramento'
                           when cv.event_object ->> 'utm_source' = 'homeadvisor' and cv.event_object ->> 'utm_location' = 'Oakland' then 'is_oakland'
                           when cv.event_object ->> 'utm_source' = 'homeadvisor' and cv.event_object ->> 'utm_location' = 'San Jose' then 'is_san_jose'
                           when cv.event_object ->> 'utm_source' = 'homeadvisor' and cv.event_object ->> 'utm_location' = 'Palo Alto' then 'is_palo_alto'
                           when cv.event_object ->> 'utm_source' = 'homeadvisor' and cv.event_object ->> 'utm_location' = 'Santa Rosa' then 'is_santa_rosa'
                           when cv.event_object ->> 'utm_source' = 'homeadvisor' and cv.event_object ->> 'utm_location' = 'Watsonville' then 'is_watsonville'
                           when cv.event_object ->> 'utm_source' = 'homeadvisor' and cv.event_object ->> 'utm_location' = 'Fresno' then 'is_fresno'
                           else 'is_other'
                   end as ha_location,
                case when l.product_id = 105 then 1 else 0 end as is_fence,
                case when l.product_id = 34 then 1 else 0 end as is_driveway
          from core_lead l        
        left join customers_visitoraction cv on cv.id = l.visitor_action_id
        where
                (phone_number is not null or email is not null)
            and full_name not ilike '%test%' and coalesce(email,'') not ilike '%test%'
            and full_name not ilike '%fake%' and coalesce(email,'') not ilike '%fake%'
                  and full_name not ilike '%duplicate%'
            and l.created_at >= '2018-04-16'
            and cv.event_object ->> 'utm_source' = 'homeadvisor'
)
select
        date_trunc('{period}',created_at at time zone 'America/Los_Angeles')::date as date,
        -- Home Advisor Leads by Location
        sum(case when ha_location = 'is_sacramento' then 1 else 0 end) as MAR197, -- sacramento_ha_leads
        sum(case when ha_location = 'is_oakland' then 1 else 0 end) as MAR198, -- oakland_ha_leads
        sum(case when ha_location = 'is_san_jose' then 1 else 0 end) as MAR199, -- san_jose_ha_leads
        sum(case when ha_location = 'is_palo_alto' then 1 else 0 end) as MAR200, -- palo_alto_ha_leads
        sum(case when ha_location = 'is_santa_rosa' then 1 else 0 end) as MAR201, -- santa_rosa_ha_leads
        sum(case when ha_location = 'is_watsonville' then 1 else 0 end) as MAR202, -- watsonville_ha_leads
        sum(case when ha_location = 'is_fresno' then 1 else 0 end) as MAR203, -- fresno_ha_leads
        sum(case when ha_location = 'is_other' then 1 else 0 end) as MAR204, -- other_ha_leads
        -- Home Advisor Fence Leads by Location
        sum(case when ha_location = 'is_sacramento' and is_fence = 1 then 1 else 0 end) as MAR197F, -- sacramento_ha_fence_leads
        sum(case when ha_location = 'is_oakland' and is_fence = 1 then 1 else 0 end) as MAR198F, -- oakland_ha_fence_leads
        sum(case when ha_location = 'is_san_jose' and is_fence = 1 then 1 else 0 end) as MAR199F, -- san_jose_ha_fence_leads
        sum(case when ha_location = 'is_palo_alto' and is_fence = 1 then 1 else 0 end) as MAR200F, -- palo_alto_ha_fence_leads
        sum(case when ha_location = 'is_santa_rosa' and is_fence = 1 then 1 else 0 end) as MAR201F, -- santa_rosa_ha_fence_leads
        sum(case when ha_location = 'is_watsonville' and is_fence = 1 then 1 else 0 end) as MAR202F, -- watsonville_ha_fence_leads
        sum(case when ha_location = 'is_fresno' and is_fence = 1 then 1 else 0 end) as MAR203F, -- fresno_ha_fence_leads
        sum(case when ha_location = 'is_other' and is_fence = 1 then 1 else 0 end) as MAR204F, -- other_ha_fence_leads
        -- Home Advisor Driveway Leads by Location
        sum(case when ha_location = 'is_sacramento' and is_driveway = 1 then 1 else 0 end) as MAR197D, -- sacramento_ha_driveway_leads
        sum(case when ha_location = 'is_oakland' and is_driveway = 1 then 1 else 0 end) as MAR198D, -- oakland_ha_driveway_leads
        sum(case when ha_location = 'is_san_jose' and is_driveway = 1 then 1 else 0 end) as MAR199D, -- san_jose_ha_driveway_leads
        sum(case when ha_location = 'is_palo_alto' and is_driveway = 1 then 1 else 0 end) as MAR200D, -- palo_alto_ha_driveway_leads
        sum(case when ha_location = 'is_santa_rosa' and is_driveway = 1 then 1 else 0 end) as MAR201D, -- santa_rosa_ha_driveway_leads
        sum(case when ha_location = 'is_watsonville' and is_driveway = 1 then 1 else 0 end) as MAR202D, -- watsonville_ha_driveway_leads
        sum(case when ha_location = 'is_fresno' and is_driveway = 1 then 1 else 0 end) as MAR203D, -- fresno_ha_driveway_leads
        sum(case when ha_location = 'is_other' and is_driveway = 1 then 1 else 0 end) as MAR204D -- other_ha_driveway_leads        
from calc_leads_detailed
group by 1
order by 1 desc
limit 40
