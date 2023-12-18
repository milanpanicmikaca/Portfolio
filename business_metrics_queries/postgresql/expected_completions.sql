with installation_bookings_initial as
-- query to filter all non cancelled Installation appointments for the two weeks
(
	select 
  		sa.date,
  		sa.order_id,
  		sa.contractor_order_id,
  		row_number() over (partition by sa.order_id) as rank_ --rank to chose unique appointment per order_id
	from schedule_appointment sa 
	where sa.contractor_order_id is not null --to filter the schedules on crew booking callendar
 		and sa.cancel_reason_id is null --non cancelled schedules
  		and sa.appointment_type_id = 5 --Installation appointment
  		and case
	  			when '{period}' = 'day' then sa.date - (now() at time zone 'America/Los_Angeles')::date = 0
	  			when '{period}' = 'week' then sa.date - (now() at time zone 'America/Los_Angeles')::date between 0 and 6
				when '{period}' in ('month','quarter','year') then 
							sa.date - (now() at time zone 'America/Los_Angeles')::date between 0 and 13
							and date_trunc('{period}',date)=date_trunc('{period}',(now() at time zone 'America/Los_Angeles')::date) -- to consider the remainder of the period
			end
),
installation_bookings_final as
--query to isolate unique order ids with their contractor id
(
	select
		date,
		order_id,
		contractor_order_id
	from installation_bookings_initial
	where rank_ = 1
),
crew_order as 
( --crew per order, needed for mapping afterwards the crews on the markets they operate, via the orders
  select
  	date,
    co.order_id,
    co.crew_id,
    cc.status_id as contractor_status
  from installation_bookings_final ibf
  left join contractor_contractororder co on co.id = ibf.contractor_order_id
  left join contractor_contractor cc on cc.id = co.contractor_id 
  where co.status_id in (3,13,66)
),
crew_market_initial as
( -- map crew with marker
  select 
  	date,
    cro.crew_id,
    contractor_status,
    pm.id as market_id,
    count(cro.order_id) as projects_scheduled_per_market
  from crew_order cro
  left join store_order so on so.id = cro.order_id
  left join core_house ch on ch.id = so.house_id
  left join geo_address ga on ga.id = ch.address_id
  left join geo_county gc on gc.id = ga.county_id
  left join product_countymarket pcm on pcm.county_id = gc.id
  left join product_market pm on pm.id = pcm.market_id
  group by 1,2,3,4
 ),
 projects_scheduled_initial as
 (--projects scheduled per market
 	select
 		date_trunc('{period}',now() at time zone 'America/Los_Angeles')::date as date,
 		crew_id,
 		case 
	    	when market_id = 1 then 'CS-SD'
            when market_id = 2 then 'CN-EB'
            when market_id = 3 then 'CN-SA'
            when market_id = 4 then 'CN-WA'
            when market_id = 5 then 'CS-OC'
            when market_id = 6 then 'CS-LA'
            when market_id = 7 then 'CS-VC'
            when market_id = 8 then 'CN-SF'
            when market_id = 9 then 'CN-NB'
            when market_id = 10 then 'CN-FR'
            when market_id = 11 then 'CS-CC'
            when market_id = 12 then 'CS-CV'
            when market_id = 13 then 'CN-NC'
            when market_id = 14 then 'CS-SV'
            when market_id = 16 then 'TX-DL'
            when market_id = 17 then 'TX-FW'
            when market_id = 18 then 'TX-HT'
            when market_id = 19 then 'TX-SA'
            when market_id = 20 then 'GA-AT'
            when market_id = 21 then 'MD-DC'
            when market_id = 22 then 'MD-BL'
            when market_id = 29 then 'CN-ST'
            when market_id = 30 then 'CN-SJ'
            when market_id = 31 then 'CN-PA'
            when market_id = 32 then 'TX-AU'
            when market_id = 33 then 'PA-PH'
            when market_id = 35 then 'VA-AR'
            when market_id = 24 then 'FL-MI'
            when market_id = 26 then 'FL-OR'
            when market_id = 43 then 'WA-SE'
            when market_id = 42 then 'WN-CH'
            when market_id = 57 then 'WN-NA'
            when market_id = 58 then 'WN-LA'
            else null 
        end as market,
 		contractor_status,
 		-- trial contractors (contractor_status = 74) will have 1 project scheduled per week
 		case 
  			when contractor_status = 74 and '{period}' = 'day' then
  			case 
      			when date_part('dow',now() at time zone 'America/Los_Angeles') = 1 then 0.199 --Monday
      			when date_part('dow',now() at time zone 'America/Los_Angeles') = 2 then 0.163 --Tuesday
      			when date_part('dow',now() at time zone 'America/Los_Angeles') = 3 then 0.169 --Wednesday
      			when date_part('dow',now() at time zone 'America/Los_Angeles') = 4 then 0.154 --Thursday
      			when date_part('dow',now() at time zone 'America/Los_Angeles') = 5 then 0.181 --Friday
      			when date_part('dow',now() at time zone 'America/Los_Angeles') = 6 then 0.094 --Saturday
      			when date_part('dow',now() at time zone 'America/Los_Angeles') = 0 then 0.040 --Sunday
      		end --weekdays projects scheduled will have more weight that weekend's projects scheduled
  			when contractor_status = 74 and '{period}' = 'week' then 1
  			when contractor_status = 74 and '{period}' in ('month','quarter','year') then 2 -- 2 weeks of projects scheduled
  			else sum(projects_scheduled_per_market)
 		end as projects_scheduled_per_market
  	from crew_market_initial
 	group by 1,2,3,4
),
projects_scheduled_final as
(-- final calculations for projects scheduled
	select 
		date,
		market,
		sum(projects_scheduled_per_market) as projects_scheduled_per_market
	from projects_scheduled_initial
	group by 1,2
),
oleads as
(--cte used to identify the 1st lead of an order, will be used for excluding test orders
	select 
		order_id,
    	min(l.id) as lead_id
	from core_lead l
	where l.created_at >= '2018-04-16'
	group by 1
),
last_approved_quotes as 
(--cte that will help on excluding cancelled project, meaning those who have a cancellation quote
  select 
    o.id as order_id,
    completed_at as cancelled_at,
    is_cancellation,
    rank() over(partition by o.id order by approved_at desc,q.id desc) as approved_rank
  from 
    store_order o join 
    quote_quote q on q.order_id = o.id 
  where 
    q.created_at >= '2018-04-16'
    and approved_at is not null
),
cancelled_projects as 
(--cte that will help on excluding cancelled project, meaning those who have a cancellation quote
  select 
    * 
  from last_approved_quotes 
  where is_cancellation = true
  and approved_rank = 1
),
calc_data
as
(--initial data for completed projects
	select
		so.id,
        so.completed_at
	from store_order so
	left join last_approved_quotes laq on so.id = laq.order_id
    left join cancelled_projects cp on cp.order_id = so.id
    where so.completed_at is not null and cp.cancelled_at is null and laq.approved_rank = 1
),
initial_completion_data as
(--query that holds the initial data needed for calculation the projects completed per market
        select
                cd.id as order_id,
                date_trunc('{period}', cd.completed_at at time zone 'America/Los_Angeles')::date as date,
                case 
	            when pcnm.market_id = 1 then 'CS-SD'
                when pcnm.market_id = 2 then 'CN-EB'
                when pcnm.market_id = 3 then 'CN-SA'
                when pcnm.market_id = 4 then 'CN-WA'
                when pcnm.market_id = 5 then 'CS-OC'
                when pcnm.market_id = 6 then 'CS-LA'
                when pcnm.market_id = 7 then 'CS-VC'
                when pcnm.market_id = 8 then 'CN-SF'
                when pcnm.market_id = 9 then 'CN-NB'
                when pcnm.market_id = 10 then 'CN-FR'
                when pcnm.market_id = 11 then 'CS-CC'
                when pcnm.market_id = 12 then 'CS-CV'
                when pcnm.market_id = 13 then 'CN-NC'
                when pcnm.market_id = 14 then 'CS-SV'
                when pcnm.market_id = 16 then 'TX-DL'
                when pcnm.market_id = 17 then 'TX-FW'
                when pcnm.market_id = 18 then 'TX-HT'
                when pcnm.market_id = 19 then 'TX-SA'
                when pcnm.market_id = 20 then 'GA-AT'
                when pcnm.market_id = 21 then 'MD-DC'
                when pcnm.market_id = 22 then 'MD-BL'
                when pcnm.market_id = 29 then 'CN-ST'
                when pcnm.market_id = 30 then 'CN-SJ'
                when pcnm.market_id = 31 then 'CN-PA'
                when pcnm.market_id = 32 then 'TX-AU'
                when pcnm.market_id = 33 then 'PA-PH'
                when pcnm.market_id = 35 then 'VA-AR'
                when pcnm.market_id = 24 then 'FL-MI'
                when pcnm.market_id = 26 then 'FL-OR'
                when pcnm.market_id = 43 then 'WA-SE'
                when pcnm.market_id = 42 then 'WN-CH'
                when pcnm.market_id = 57 then 'WN-NA'
                when pcnm.market_id = 58 then 'WN-LA'
                else null end as market,
                o.parent_order_id
        from calc_data cd
        left join store_order o on cd.id = o.id
        left join quote_quote q on q.id = o.approved_quote_id
        left join core_house h on h.id = o.house_id
        left join geo_address ga on ga.id = h.address_id
        left join geo_county cn on cn.id = ga.county_id
        left join product_countymarket pcnm on pcnm.county_id = cn.id
        left join product_market pm on pm.id = pcnm.market_id
        left join oleads l on l.order_id = o.id
        left join core_lead cl on cl.id = l.lead_id
        left join customers_contact co on co.id = cl.contact_id
        left join core_user cu on cu.id = co.user_id
        left join cancelled_projects cp on cp.order_id = cd.id
        where
                o.completed_at is not null
                and cp.cancelled_at is null
                and q.approved_at >= '2018-04-15'
                and o.id not in (50815,56487,59225,59348,59404,59666,59670,59743,59753,
                                                        59789,59805,59813,59878,59908,59922,60273,60283,60401,60547,60589,60590,60595,60596,60597,60612)
                and coalesce(cl.full_name,'')||coalesce(co.full_name,'')||coalesce(cu.full_name,'') not ilike '%[TEST]%'
                and coalesce(cl.email,'')||coalesce(cu.email,'') not ilike '%+test%'
),
order_completed_initial as
(
	select 
		date,
		market,
		count(order_id) as projects_completed_per_market,
		dense_rank() over (order by date desc) as date_rank
	from initial_completion_data
	where parent_order_id is null
	group by 1,2
),
order_completed_final as
(
	select
		date,
		market,
		projects_completed_per_market
	from order_completed_initial
	where date_rank = 1
),
merged_data_initial as
(--merging projects scheduled and completed by market, per period
	select
		coalesce(ps.date,oc.date) as date,
		coalesce(ps.market,oc.market) as market,
		coalesce(projects_scheduled_per_market,0) as projects_scheduled_per_market,
		coalesce(projects_completed_per_market,0) as projects_completed_per_market
	from projects_scheduled_final ps
	full join order_completed_final oc on oc.date = ps.date and oc.market = ps.market
),
dates_calculation_cte as
(--cte used for the calculation of dates that will be used in the next cte
	select
		date,
		date_part('days',now() at time zone 'America/Los_Angeles' + interval '1 month') as days_of_month,
		date_part('days',now() at time zone 'America/Los_Angeles') as days_passed_in_month,
		(now() at time zone 'America/Los_Angeles')::date - 
			date_trunc('quarter', now() at time zone 'America/Los_Angeles')::date + 1 as days_passed_in_quarter,
		(date_trunc('quarter', now() at time zone 'America/Los_Angeles')+interval '3 months')::date 
			- date_trunc('quarter', now() at time zone 'America/Los_Angeles')::date + 1 as days_of_quarter,
		(now() at time zone 'America/Los_Angeles')::date
			- date_trunc('year', now() at time zone 'America/Los_Angeles')::date + 1 as days_passed_in_year,
		(date_trunc('year', now() at time zone 'America/Los_Angeles')+interval '1 year')::date
			- date_trunc('year', now() at time zone 'America/Los_Angeles')::date + 1 as days_of_year,
		market,
		projects_scheduled_per_market,
		projects_completed_per_market
	from merged_data_initial
),
initial_expected_completion_data as
(
select
	date,
	market,
	cast(case
		when '{period}' = 'day' then projects_scheduled_per_market
		when '{period}' = 'week' then projects_scheduled_per_market
		when '{period}' = 'month' then
			case 
				when days_of_month - days_passed_in_month >= 14
					then projects_completed_per_market + projects_scheduled_per_market * (days_of_month - days_passed_in_month) / 14
				else projects_completed_per_market + projects_scheduled_per_market
			end
		when '{period}' = 'quarter' then 
			case 
				when days_of_quarter - days_passed_in_quarter >= 14
					then projects_completed_per_market + projects_scheduled_per_market * (days_of_quarter - days_passed_in_quarter) / 14
				else projects_completed_per_market + projects_scheduled_per_market
			end
		when '{period}' = 'year' then
			case 
				when days_of_year - days_passed_in_year >= 14
					then projects_completed_per_market + projects_scheduled_per_market * (days_of_year - days_passed_in_year) / 14
				else projects_completed_per_market + projects_scheduled_per_market
			end
	end as numeric) as expected_completions
from dates_calculation_cte
),
expected_completions_per_market as
(
select
  date,
  round(sum(case when market = 'CN-EB' then expected_completions else 0 end),0) as DEL522, --eb_expected_completions
  round(sum(case when market = 'CN-NB' then expected_completions else 0 end),0) as DEL523, --nb_expected_completions
  round(sum(case when market = 'CN-SA' then expected_completions else 0 end),0) as DEL524, --sac_expected_completions
  round(sum(case when market in ('CN-WA','CN-SJ','CN-PA') then expected_completions else 0 end),0) as DEL525, --sb_expected_completions
  round(sum(case when market = 'CN-SF' then expected_completions else 0 end),0) as DEL526, --sf_expected_completions
  round(sum(case when market = 'CN-FR' then expected_completions else 0 end),0) as DEL527, --fr_expected_completions
  round(sum(case when market = 'CN-WA' then expected_completions else 0 end),0) as DEL528, --wa_expected_completions
  round(sum(case when market = 'CN-SJ' then expected_completions else 0 end),0) as DEL529, --sj_expected_completions
  round(sum(case when market = 'CN-PA' then expected_completions else 0 end),0) as DEL530, --pa_expected_completions
  round(sum(case when market = 'CN-ST' then expected_completions else 0 end),0) as DEL531, --st_expected_completions
  round(sum(case when market in ('CN-WA','CN-SJ','CN-PA','CN-SF') then expected_completions else 0 end),0) as DEL532, --sb_sf_expected_completions
  round(sum(case when market = 'CS-SV' then expected_completions else 0 end),0) as DEL534, --sv_expected_completions
  round(sum(case when market = 'CS-OC' then expected_completions else 0 end),0) as DEL535, --oc_expected_completions
  round(sum(case when market = 'CS-LA' then expected_completions else 0 end),0) as DEL536, --la_expected_completions
  round(sum(case when market = 'CS-VC' then expected_completions else 0 end),0) as DEL537, --vc_expected_completions
  round(sum(case when market = 'CS-SD' then expected_completions else 0 end),0) as DEL538, --sd_expected_completions
  round(sum(case when market = 'TX-FW' then expected_completions else 0 end),0) as DEL540, --fw_expected_completions
  round(sum(case when market = 'TX-DL' then expected_completions else 0 end),0) as DEL541, --dl_expected_completions
  round(sum(case when market = 'TX-SA' then expected_completions else 0 end),0) as DEL542, --sa_expected_completions
  round(sum(case when market = 'TX-HT' then expected_completions else 0 end),0) as DEL543, --ht_expected_completions
  round(sum(case when market = 'TX-AU' then expected_completions else 0 end),0) as DEL544, --au_expected_completions
  round(sum(case when market = 'GA-AT' then expected_completions else 0 end),0) as DEL546, --at_expected_completions
  round(sum(case when market = 'MD-BL' then expected_completions else 0 end),0) as DEL549, --bl_expected_completions
  round(sum(case when market = 'MD-DC' then expected_completions else 0 end),0) as DEL550, --dc_expected_completions
  round(sum(case when market = 'PA-PH' then expected_completions else 0 end),0) as DEL552, --ph_expected_completions
  round(sum(case when market = 'VA-AR' then expected_completions else 0 end),0) as DEL554, --ar_expected_completions
  round(sum(case when market = 'FL-MI' then expected_completions else 0 end),0) as DEL556, --mi_expected_completions
  round(sum(case when market = 'FL-OR' then expected_completions else 0 end),0) as DEL557, --or_expected_completions
  round(sum(case when market = 'WA-SE' then expected_completions else 0 end),0) as DEL559, --se_expected_completions
  round(sum(case when market = 'WN-CH' then expected_completions else 0 end),0) as DEL581, --wn_ch_expected_completions
  round(sum(case when market = 'WN-NA' then expected_completions else 0 end),0) as DEL592, --wn_na_expected_completions
  round(sum(case when market = 'WN-LA' then expected_completions else 0 end),0) as DEL603 --wn_la_expected_completions
from initial_expected_completion_data
group by 1
),
expected_completions_per_region as
(--calculations this way prevent inaccuracies of the expected completions of regions due to the round function
select *,
	DEL522+DEL523+DEL524+DEL526+DEL527+DEL528+DEL529+DEL530+DEL531 as DEL521, --nc_expected_completions
	DEL534+DEL535+DEL536+DEL537+DEL538 as DEL533, --sc_expected_completions
	DEL540+DEL541+DEL542+DEL543+DEL544 as DEL539, --tx_expected_completions
	DEL546 as DEL545, --ga_expected_completions,
	DEL549+DEL550+DEL552+DEL554 as DEL547, --ne_expected_completions
	DEL556+DEL557 as DEL555, --fl_expected_completions
	DEL559 as DEL558, --wa_expected_completions
	DEL581+DEL592+DEL603 as DEL570 -- wn_expected_completions
from expected_completions_per_market
)
select *,
	-- total expected completions, calculation this way to avoid inaccuracies
	DEL521+DEL533+DEL539+DEL545+DEL547+DEL555+DEL558+DEL570 as DEL520
from expected_completions_per_region
