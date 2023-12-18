-- upload to BQ
with
calc_leads_detailed as
(
        select
                   l.id,
                   l.created_at,
                   case 
                           when cv.event_object ->> 'utm_source' ilike '%yelp%' and cv.event_object ->> 'utm_location' = 'Sacramento' then 'is_sacramento'
                           when cv.event_object ->> 'utm_source' ilike '%yelp%' and cv.event_object ->> 'utm_location' = 'Oakland' then 'is_oakland'
                           when cv.event_object ->> 'utm_source' ilike '%yelp%' and cv.event_object ->> 'utm_location' = 'San Jose' then 'is_san_jose'
                           when cv.event_object ->> 'utm_source' ilike '%yelp%' and cv.event_object ->> 'utm_location' = 'Palo Alto' then 'is_palo_alto'
                           when cv.event_object ->> 'utm_source' ilike '%yelp%' and cv.event_object ->> 'utm_location' = 'Napa' then 'is_napa'
                           when cv.event_object ->> 'utm_source' ilike '%yelp%' and cv.event_object ->> 'utm_location' = 'Watsonville' then 'is_watsonville'
                           when cv.event_object ->> 'utm_source' ilike '%yelp%' and cv.event_object ->> 'utm_location' = 'Fresno' then 'is_fresno'
                           else 'is_other'
                   end as yelp_location,
                case when l.product_id = 105 then 1 else 0 end as is_fence,
                case when l.product_id = 34 then 1 else 0 end as is_driveway,
                is_commercial::integer
          from core_lead l        
        left join customers_visitoraction cv on cv.id = l.visitor_action_id
        left join store_order o on o.id = l.order_id 
        left join core_house ch on ch.id = o.house_id
        left join customers_customer cc on cc.id = ch.customer_id
        where
                (phone_number is not null or email is not null)
            and full_name not ilike '%test%' and coalesce(email,'') not ilike '%test%'
            and full_name not ilike '%fake%' and coalesce(email,'') not ilike '%fake%'
                  and full_name not ilike '%duplicate%'
            and l.created_at >= '2018-04-16'
            and cv.event_object ->> 'utm_source' ilike '%yelp%'
)
select
        date_trunc('{period}',created_at at time zone 'America/Los_Angeles')::date as date,
        -- Yelp Leads by Location
        sum(case when yelp_location = 'is_sacramento' then 1 else 0 end) as MAR220, -- sacramento_yelp_leads
        sum(case when yelp_location = 'is_oakland' then 1 else 0 end) as MAR221, -- oakland_yelp_leads
        sum(case when yelp_location = 'is_san_jose' then 1 else 0 end) as MAR222, -- san_jose_yelp_leads
        sum(case when yelp_location = 'is_palo_alto' then 1 else 0 end) as MAR223, -- palo_alto_yelp_leads
        sum(case when yelp_location = 'is_napa' then 1 else 0 end) as MAR224, -- napa_yelp_leads
        sum(case when yelp_location = 'is_watsonville' then 1 else 0 end) as MAR225, -- watsonville_yelp_leads
        sum(case when yelp_location = 'is_fresno' then 1 else 0 end) as MAR226, -- fresno_yelp_leads
        sum(case when yelp_location = 'is_other' then 1 else 0 end) as MAR227, -- other_yelp_leads
        -- Yelp Fence Leads by Location 
        sum(case when yelp_location = 'is_sacramento' and is_fence = 1 and is_commercial = 0 then 1 else 0 end) as MAR220F, -- sacramento_yelp_fence_leads
        sum(case when yelp_location = 'is_oakland' and is_fence = 1 and is_commercial = 0 then 1 else 0 end) as MAR221F, -- oakland_yelp_fence_leads
        sum(case when yelp_location = 'is_san_jose' and is_fence = 1 and is_commercial = 0 then 1 else 0 end) as MAR222F, -- san_jose_yelp_fence_leads
        sum(case when yelp_location = 'is_palo_alto' and is_fence = 1 and is_commercial = 0 then 1 else 0 end) as MAR223F, -- palo_alto_yelp_fence_leads
        sum(case when yelp_location = 'is_napa' and is_fence = 1 and is_commercial = 0 then 1 else 0 end) as MAR224F, -- napa_yelp_fence_leads
        sum(case when yelp_location = 'is_watsonville' and is_fence = 1 and is_commercial = 0 then 1 else 0 end) as MAR225F, -- watsonville_yelp_fence_leads
        sum(case when yelp_location = 'is_fresno' and is_fence = 1 and is_commercial = 0 then 1 else 0 end) as MAR226F, -- fresno_yelp_fence_leads
        sum(case when yelp_location = 'is_other' and is_fence = 1 and is_commercial = 0 then 1 else 0 end) as MAR227F, -- other_yelp_fence_leads
        -- Yelp Driveway Leads by Location
        sum(case when yelp_location = 'is_sacramento' and is_driveway = 1 and is_commercial = 0 then 1 else 0 end) as MAR220D, -- sacramento_yelp_driveway_leads
        sum(case when yelp_location = 'is_oakland' and is_driveway = 1 and is_commercial = 0 then 1 else 0 end) as MAR221D, -- oakland_yelp_driveway_leads
        sum(case when yelp_location = 'is_san_jose' and is_driveway = 1 and is_commercial = 0 then 1 else 0 end) as MAR222D, -- san_jose_yelp_driveway_leads
        sum(case when yelp_location = 'is_palo_alto' and is_driveway = 1 and is_commercial = 0 then 1 else 0 end) as MAR223D, -- palo_alto_yelp_driveway_leads
        sum(case when yelp_location = 'is_napa' and is_driveway = 1 and is_commercial = 0 then 1 else 0 end) as MAR224D, -- napa_yelp_driveway_leads
        sum(case when yelp_location = 'is_watsonville' and is_driveway = 1 and is_commercial = 0 then 1 else 0 end) as MAR225D, -- watsonville_yelp_driveway_leads
        sum(case when yelp_location = 'is_fresno' and is_driveway = 1 and is_commercial = 0 then 1 else 0 end) as MAR226D, -- fresno_yelp_driveway_leads
        sum(case when yelp_location = 'is_other' and is_driveway = 1 and is_commercial = 0 then 1 else 0 end) as MAR227D -- other_yelp_driveway_leads        
from calc_leads_detailed
group by 1
order by 1 desc
