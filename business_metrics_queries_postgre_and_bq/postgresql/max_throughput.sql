with installation_bookings_initial as
-- query to filter all non cancelled Installation appointments
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
  		and sa.date - ((now() at time zone 'America/Los_Angeles')::date) between 0 and 13
),
installation_bookings_final as
--query to isolate unique order ids with their contractor id
(
	select 
		order_id,
		contractor_order_id
	from installation_bookings_initial
	where rank_ = 1
),
crew_order as 
( --crew per order, needed for mapping afterwards the crews on the markets they operate, via the orders
  select
    co.order_id,
    co.crew_id,
    cc.avg_projects_per_week
  from installation_bookings_final ibf
  left join contractor_contractororder co on co.id = ibf.contractor_order_id
  left join contractor_contractorcrew cc on cc.id = co.crew_id 
  where co.status_id in (3,13,66) and cc.avg_projects_per_week is not null and cc.avg_projects_per_week <> 0
),
crew_market_initial as
(--map crews with the market they operate via the orders and count the projects per market per crew
  select 
    cro.crew_id,
    pm.id as market_id,
    cro.avg_projects_per_week,
    count(cro.order_id) as projects_scheduled_per_market
  from crew_order cro
  left join store_order so on so.id = cro.order_id
  left join core_house ch on ch.id = so.house_id
  left join geo_address ga on ga.id = ch.address_id
  left join geo_county gc on gc.id = ga.county_id
  left join product_countymarket pcm on pcm.county_id = gc.id
  left join product_market pm on pm.id = pcm.market_id
  group by 1,2,3
),
crew_market_final as
(--calculate the weighted capacity of a crew per market
	select 
		crew_id,
		market_id,
		--formula: admin crew capacity * (projects scheduled per market per crew / pojects scheduled per crew)
		avg_projects_per_week*(projects_scheduled_per_market/sum(projects_scheduled_per_market) over (partition by crew_id)) as capacity_per_week
	from crew_market_initial
),
initial_capacity_calc as
(-- calculate the total capacity per market, for the specific period
  select
    date_trunc('{period}',now() at time zone 'America/Los_Angeles')::date as date,
    case 
      when cm.market_id = 1 then 'CS-SD'
      when cm.market_id = 2 then 'CN-EB'
      when cm.market_id = 3 then 'CN-SA'
      when cm.market_id = 4 then 'CN-WA'
      when cm.market_id = 5 then 'CS-OC'
      when cm.market_id = 6 then 'CS-LA'
      when cm.market_id = 7 then 'CS-VC'
      when cm.market_id = 8 then 'CN-SF'
      when cm.market_id = 9 then 'CN-NB'
      when cm.market_id = 10 then 'CN-FR'
      when cm.market_id = 11 then 'CS-CC'
      when cm.market_id = 12 then 'CS-CV'
      when cm.market_id = 13 then 'CN-NC'
      when cm.market_id = 14 then 'CS-SV'
      when cm.market_id = 16 then 'TX-DL'
      when cm.market_id = 17 then 'TX-FW'
      when cm.market_id = 18 then 'TX-HT'
      when cm.market_id = 19 then 'TX-SA'
      when cm.market_id = 20 then 'GA-AT'
      when cm.market_id = 21 then 'MD-DC'
      when cm.market_id = 22 then 'MD-BL'
      when cm.market_id = 29 then 'CN-ST'
      when cm.market_id = 30 then 'CN-SJ'
      when cm.market_id = 31 then 'CN-PA'
      when cm.market_id = 32 then 'TX-AU'
      when cm.market_id = 33 then 'PA-PH'
      when cm.market_id = 35 then 'VA-AR'
      when cm.market_id = 24 then 'FL-MI'
      when cm.market_id = 26 then 'FL-OR'
      when cm.market_id = 43 then 'WA-SE'
      when cm.market_id = 42 then 'WN-CH'
      when cm.market_id = 57 then 'WN-NA'
      when cm.market_id = 58 then 'WN-LA'
    else null end as market,
    cast(case
      when '{period}' = 'day' then
      	case 
      		when date_part('dow',(now() at time zone 'America/Los_Angeles')::date) = 1 then sum(cm.capacity_per_week)*0.199 --Monday
      		when date_part('dow',(now() at time zone 'America/Los_Angeles')::date) = 2 then sum(cm.capacity_per_week)*0.163	--Tuesday
      		when date_part('dow',(now() at time zone 'America/Los_Angeles')::date) = 3 then sum(cm.capacity_per_week)*0.169	--Wednesday
      		when date_part('dow',(now() at time zone 'America/Los_Angeles')::date) = 4 then sum(cm.capacity_per_week)*0.154	--Thursday
      		when date_part('dow',(now() at time zone 'America/Los_Angeles')::date) = 5 then sum(cm.capacity_per_week)*0.181	--Friday
      		when date_part('dow',(now() at time zone 'America/Los_Angeles')::date) = 6 then sum(cm.capacity_per_week)*0.094 --Saturday
      		when date_part('dow',(now() at time zone 'America/Los_Angeles')::date) = 0 then sum(cm.capacity_per_week)*0.040	--Sunday
      	end --weekdays capacity will have more weight that weekend's capacity
      when '{period}' = 'week' then sum(cm.capacity_per_week)
      when '{period}' = 'month' 
        then sum(cm.capacity_per_week)/7*extract(day from date_trunc('month', (now() at time zone 'America/Los_Angeles')::date) + interval '1 month - 1 day') --last day of the month
      when '{period}' = 'quarter' 
        then sum(cm.capacity_per_week)/7*extract(day from date_trunc('quarter', (now() at time zone 'America/Los_Angeles')::date) + interval '3 month - 1 day') --last day of the quarter
      when '{period}' = 'year' 
        then sum(cm.capacity_per_week)/7*extract(day from date_trunc('year', (now() at time zone 'America/Los_Angeles')::date) + interval '12 month - 1 day') --last day of the year
    end as numeric) as capacity 
  from crew_market_final cm
  where cm.market_id is not null
  group by 1,2
),
capacity_per_market_calc as
(
select
  date,
  round(sum(case when market = 'CN-EB' then capacity else 0 end),0) as DEL482, --eb_capacity
  round(sum(case when market = 'CN-NB' then capacity else 0 end),0) as DEL483, --nb_capacity
  round(sum(case when market = 'CN-SA' then capacity else 0 end),0) as DEL484, --sac_capacity
  round(sum(case when market in ('CN-WA','CN-SJ','CN-PA') then capacity else 0 end),0) as DEL485, --sb_capacity
  round(sum(case when market = 'CN-SF' then capacity else 0 end),0) as DEL486, --sf_capacity
  round(sum(case when market = 'CN-FR' then capacity else 0 end),0) as DEL487, --fr_capacity
  round(sum(case when market = 'CN-WA' then capacity else 0 end),0) as DEL488, --wa_capacity
  round(sum(case when market = 'CN-SJ' then capacity else 0 end),0) as DEL489, --sj_capacity
  round(sum(case when market = 'CN-PA' then capacity else 0 end),0) as DEL490, --pa_capacity
  round(sum(case when market = 'CN-ST' then capacity else 0 end),0) as DEL491, --st_capacity
  round(sum(case when market in ('CN-WA','CN-SJ','CN-PA','CN-SF') then capacity else 0 end),0) as DEL492, --sb_sf_capacity
  round(sum(case when market = 'CS-SV' then capacity else 0 end),0) as DEL494, --sv_capacity
  round(sum(case when market = 'CS-OC' then capacity else 0 end),0) as DEL495, --oc_capacity
  round(sum(case when market = 'CS-LA' then capacity else 0 end),0) as DEL496, --la_capacity
  round(sum(case when market = 'CS-VC' then capacity else 0 end),0) as DEL497, --vc_capacity
  round(sum(case when market = 'CS-SD' then capacity else 0 end),0) as DEL498, --sd_capacity
  round(sum(case when market = 'TX-FW' then capacity else 0 end),0) as DEL500, --fw_capacity
  round(sum(case when market = 'TX-DL' then capacity else 0 end),0) as DEL501, --dl_capacity
  round(sum(case when market = 'TX-SA' then capacity else 0 end),0) as DEL502, --sa_capacity
  round(sum(case when market = 'TX-HT' then capacity else 0 end),0) as DEL503, --ht_capacity
  round(sum(case when market = 'TX-AU' then capacity else 0 end),0) as DEL504, --au_capacity
  round(sum(case when market = 'GA-AT' then capacity else 0 end),0) as DEL506, --at_capacity
  round(sum(case when market = 'MD-BL' then capacity else 0 end),0) as DEL509, --bl_capacity
  round(sum(case when market = 'MD-DC' then capacity else 0 end),0) as DEL510, --dc_capacity
  round(sum(case when market = 'PA-PH' then capacity else 0 end),0) as DEL512, --ph_capacity
  round(sum(case when market = 'VA-AR' then capacity else 0 end),0) as DEL514, --ar_capacity
  round(sum(case when market = 'FL-MI' then capacity else 0 end),0) as DEL516, --mi_capacity
  round(sum(case when market = 'FL-OR' then capacity else 0 end),0) as DEL517, --or_capacity
  round(sum(case when market = 'WA-SE' then capacity else 0 end),0) as DEL519, --se_capacity
  round(sum(case when market = 'WN-CH' then capacity else 0 end),0) as DEL580, --wn_il_ch_capacity
  round(sum(case when market = 'WN-NA' then capacity else 0 end),0) as DEL591, --wn_il_na_capacity
  round(sum(case when market = 'WN-LA' then capacity else 0 end),0) as DEL602 --wn_il_la_capacity
from initial_capacity_calc
group by 1
),
capacity_per_region_calc as
(--calculations done this way to prevent inaccuracies to addition of the capacity of markets to regions 
  --due to the round function
select *,
	DEL482+DEL483+DEL484+DEL486+DEL487+DEL488+DEL489+DEL490+DEL491 as DEL481, --nc_capacity
	DEL494+DEL495+DEL496+DEL497+DEL498 as DEL493, --sc_capacity
	DEL500+DEL501+DEL502+DEL503+DEL504 as DEL499, --tx_capacity
	DEL506 as DEL505, --ga_capacity,
	DEL509+DEL510+DEL512+DEL514 as DEL507, --ne_capacity
	DEL516+DEL517 as DEL515, --fl_capacity
	DEL519 as DEL518, --wa_capacity
	DEL580 + DEL591 + DEL602 as DEL569 --il capacity
from capacity_per_market_calc
)-- total capacity, calculation done this way to avoid inaccuracies with the round function
select *,
	DEL481+DEL493+DEL499+DEL505+DEL507+DEL515+DEL518+DEL569 as DEL480
from capacity_per_region_calc
