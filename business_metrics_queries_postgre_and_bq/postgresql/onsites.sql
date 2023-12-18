-- upload to BQ
with
calc_approved as
(
        select
                id,
            order_id,
            approved_at
        from
          (
                  select
                      q.*,
                     rank() over (partition by q.order_id order by q.approved_at) as rank
            from quote_quote q
            where
                     q.approved_at is not null
          ) as k
        where
                rank = 1
),
min_lead_service as --in cases of a lead with multiple services we grab the one with the smallest ID
(
select
	lead_id,
	min(cls.id) as first_lead_service
from
	core_lead_services cls
group by 1
),
calc_lead as
(
        select *
          from
           (
                select
                      l.id,
                l.created_at,
                l.address_id,
                l.order_id,
                ps.label as service_category,
                rank() over (partition by l.order_id order by l.created_at) as rank
            from core_lead l
                left join customers_visitoraction cv on cv.id = l.visitor_action_id
                left join min_lead_service ml on l.id = ml.lead_id
       	        left join core_lead_services cls on cls.id = ml.first_lead_service
                left join product_service ps on ps.id = cls.service_id
           ) as k
        where
                 rank = 1
),
calc_booking as
(
        select
                   ssa.*,
                   ga.formatted_address,
                   case when o.product_id = 105 then 'Fence' when o.product_id = 34 then 'Driveway'
                   when o.product_id = 132 then 'Turf' else 'Fence' end as product_name,
             pm.code as market_code,
              case when pcm.market_id = 1 then 'CS-SD'
                when pcm.market_id = 2 then 'CN-EB'
                when pcm.market_id = 3 then 'CN-SA'
                when pcm.market_id = 4 then 'CN-WA'
                when pcm.market_id = 5 then 'CS-OC'
                when pcm.market_id = 6 then 'CS-LA'
                when pcm.market_id = 7 then 'CS-VC'
                when pcm.market_id = 8 then 'CN-SF'
                when pcm.market_id = 9 then 'CN-NB'
                when pcm.market_id = 10 then 'CN-FR'
                when pcm.market_id = 11 then 'CS-CC'
                when pcm.market_id = 12 then 'CS-CV'
                when pcm.market_id = 13 then 'CN-NC'
                when pcm.market_id = 14 then 'CS-SV'
                when pcm.market_id = 16 then 'TX-DL'
                when pcm.market_id = 17 then 'TX-FW'
                when pcm.market_id = 18 then 'TX-HT'
                when pcm.market_id = 19 then 'TX-SA'
                when pcm.market_id = 20 then 'GA-AT'
                when pcm.market_id = 21 then 'MD-DC'
                when pcm.market_id = 22 then 'MD-BL'
                when pcm.market_id = 29 then 'CN-ST'
                when pcm.market_id = 30 then 'CN-SJ'
                when pcm.market_id = 31 then 'CN-PA'
                when pcm.market_id = 32 then 'TX-AU'
                when pcm.market_id = 33 then 'PA-PH'
                when pcm.market_id = 35 then 'VA-AR'
                when pcm.market_id = 24 then 'FL-MI'
                when pcm.market_id = 26 then 'FL-OR'
                else null end as market,
             case when ssat.code = 'physical_onsite' then 1 else 0 end as is_physical,
             case when ssat.code = 'physical_onsite' and o.product_id = 105 then 1 else 0 end as is_fence_physical,
             case when ssat.code = 'physical_onsite' and o.product_id = 132 then 1 else 0 end as is_turf_physical,
             case when ssat.code = 'physical_onsite' and o.product_id = 34 then 1 else 0 end as is_driveway_physical,
                         case when ssat.code = 'physical_onsite' and is_commercial is true then 1 else 0 end as is_commercial_physical,
            rank() over (partition by ga.formatted_address, o.product_id order by ssa.created_at desc) as rank,
            ssat.code,
            is_commercial::integer --ADDED
          from schedule_appointment ssa
          left join store_order o on o.id = ssa.order_id
          left join core_house h on h.id = o.house_id
          left join customers_customer cc on cc.id = h.customer_id
         left join geo_address ga on ga.id = h.address_id
         left join geo_county gcn on gcn.id = ga.county_id
    left join product_countymarket pcm on pcm.county_id = gcn.id
    left join product_market pm on pm.id = pcm.market_id
    left join schedule_appointmenttype ssat on ssat.id = ssa.appointment_type_id
          where
                  ssat.code in ('remote_onsite', 'physical_onsite')
                   and ssa.date <= now() and ssa.cancelled_at is null
),
calc_last_booking as
(
        select
                 cb.*,
                   cl.service_category,
                  case when ca.approved_at - cb.date <= interval '14 days' then 1 else 0 end as a14,
        case when ca.approved_at - cb.date <= interval '28 days' then 1 else 0 end as a28,
        case when ca.approved_at - cb.date <= interval '56 days' then 1 else 0 end as a56,
        case when ca.approved_at is null then 0 else 1 end as appr
         from calc_booking cb
          left join calc_lead cl on cl.order_id = cb.order_id
          left join core_lead l on l.id = cl.id
          left join calc_approved ca on ca.order_id = cb.order_id
          left join customers_contact cco on cco.id = l.contact_id
          left join core_user cu on cu.id = cco.user_id
        where coalesce(l.full_name,'')||coalesce(cco.full_name,'')||coalesce(cu.full_name,'') not ilike '%[TEST]%'
        and coalesce(l.email,'')||coalesce(cu.email,'') not ilike '%+test%'
        and cb.rank = 1
),
includes_commercial
as
(
select
          date_trunc('{period}', date)::date as date,
          count(*) as SAL101, --vis
          sum(appr) as SAL102, --appr
          sum(a14) as SAL103, --a14
          sum(a28) as SAL104, --a28
          sum(a56) as SAL105, --a56
          sum(is_physical) filter (where cancel_reason_text is null) as SAL130 --physical_onsites
          from calc_last_booking
where
          created_at is not null
group by 1
),
excludes_commercial as
(
select
    date_trunc('{period}', date)::date as date,
    count(product_name) filter (where product_name = 'Fence') as SAL101F, --fence_vis
    count(product_name) filter (where product_name = 'Turf') as SAL101T, --turf_vis
    count(product_name) filter (where product_name = 'Driveway') as SAL101D, --driveway_vis
    sum(case when product_name = 'Fence' and service_category ilike '%vinyl%' then 1 else 0 end) as SAL458F,
    sum(case when product_name = 'Fence' then appr else 0 end) as SAL102F, --fence_appr
    sum(case when product_name = 'Fence' then a14 else 0 end) as SAL103F, --fence_a14
    sum(case when product_name = 'Fence' then a28 else 0 end) as SAL104F, --fence_a28
    sum(case when product_name = 'Fence' then a56 else 0 end) as SAL105F, --fence_a56
    sum(case when product_name = 'Turf' then appr else 0 end) as SAL102T, --turf_appr
    sum(case when product_name = 'Driveway' then appr else 0 end) as SAL102D, --driveway_appr
    sum(case when product_name = 'Driveway' then a14 else 0 end) as SAL103D, --driveway_a14
    sum(case when product_name = 'Driveway' then a28 else 0 end) as SAL104D, --driveway_a28
    sum(case when product_name = 'Driveway' then a56 else 0 end) as SAL105D, --driveway_a56
    sum(is_fence_physical) as SAL130F, --fence_physical_onsites
    coalesce(sum(is_turf_physical),0) as SAL130T, --turf_physical_onsites
    sum(is_driveway_physical) as SAL130D, --driveway_physical_onsites
    sum(is_commercial_physical) as SAL130C --commercial_physical_onsites
from calc_last_booking
where
        created_at is not null
        and (is_commercial = 0 or is_commercial is null)
group by 1)
SELECT *
from includes_commercial ic
left join excludes_commercial ec using(date)
order by 1 desc;
