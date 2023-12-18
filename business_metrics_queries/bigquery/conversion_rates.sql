-- upload to BQ
with
timeseries 
as (
select 
    date_trunc(date_array,{period}) as date,
from 
    unnest(generate_date_array('2018-04-16',current_date(), interval 1 day)) as date_array
group by 1
), 
calc_data
as
(
select  
    order_id,
    created_at,
    quoted_at,
    won_at,
    case when product like "/Driveway%" then 'driveway' 
    when product like "/Fence%" then 'fence' 
    when product like "%/Install Artificial Grass" then 'turf' end as product,
    case when type like "/Residential%" then "residential" else "commercial" end as type, 
    case when is_lead then 1 else 0 end as is_lead,
    if(date_diff(current_date(),created_at,day) >= 14,1,0) as is_order14,
    if(date_diff(current_date(),created_at,day) >= 30,1,0) as is_order30,
    if(date_diff(current_date(),created_at,day) >= 60,1,0) as is_order60,    
    if(is_lead = true and date_diff(current_date(),created_at,day) >= 14,1,0) as is_lead14,
    if(is_lead = true and date_diff(current_date(),created_at,day) >= 30,1,0) as is_lead30,
    if(is_lead = true and date_diff(current_date(),created_at,day) >= 60,1,0) as is_lead60,
    case when quoted_at is not null then 1 else 0 end as is_quoted,
    if(date_diff(current_date(),quoted_at,day) >= 14,1,0) as is_quote14,
    if(date_diff(current_date(),quoted_at,day) >= 30,1,0) as is_quote30,
    if(date_diff(current_date(),quoted_at,day) >= 60,1,0) as is_quote60,
    if(date_diff(quoted_at,created_at,day) <= 14,1,0) as is_quoted14,
    if(date_diff(quoted_at,created_at,day) <= 30,1,0) as is_quoted30,
    if(date_diff(quoted_at,created_at,day) <= 60,1,0) as is_quoted60,
    case when won_at is not null then 1 else 0 end as is_win,
    if(date_diff(won_at,quoted_at,day) <= 14,1,0) as is_won14q,
    if(date_diff(won_at,quoted_at,day) <= 30,1,0) as is_won30q,
    if(date_diff(won_at,quoted_at,day) <= 60,1,0) as is_won60q
from 
    int_data.order_ue_materialized
order by 1 desc
),
lead2quote
as
(
select 
    date_trunc(created_at,{period}) as date,
    sum(is_quoted)/nullif(sum(is_lead),0) as SAL683,
    sum(is_order14*is_quoted14)/nullif(sum(is_lead14),0) as SAL684,
    sum(is_order30*is_quoted30)/nullif(sum(is_lead30),0) as SAL685,
    sum(is_order60*is_quoted60)/nullif(sum(is_lead60),0) as SAL686
from 
    calc_data 
group by 1
),
lead2quotef
as
(
select 
    date_trunc(created_at,{period}) as date,
    sum(is_quoted)/nullif(sum(is_lead),0) as SAL683F,
    sum(is_order14*is_quoted14)/nullif(sum(is_lead14),0) as SAL684F,
    sum(is_order30*is_quoted30)/nullif(sum(is_lead30),0) as SAL685F,
    sum(is_order60*is_quoted60)/nullif(sum(is_lead60),0) as SAL686F
from 
    calc_data
where product = 'fence' 
and type = 'residential'
group by 1
),
lead2quote_t
as
(
select 
    date_trunc(created_at,{period}) as date,
    sum(is_quoted)/nullif(sum(is_lead),0) as SAL683T,
    sum(is_order14*is_quoted14)/nullif(sum(is_lead14),0) as SAL684T,
    sum(is_order30*is_quoted30)/nullif(sum(is_lead30),0) as SAL685T,
    sum(is_order60*is_quoted60)/nullif(sum(is_lead60),0) as SAL686T
from 
    calc_data
where product = 'turf' 
and type = 'residential'
group by 1
),
lead2quoted
as
(
select 
    date_trunc(created_at,{period}) as date,
    sum(is_quoted)/nullif(sum(is_lead),0) as SAL683D,
    sum(is_order14*is_quoted14)/nullif(sum(is_lead14),0) as SAL684D,
    sum(is_order30*is_quoted30)/nullif(sum(is_lead30),0) as SAL685D,
    sum(is_order60*is_quoted60)/nullif(sum(is_lead60),0) as SAL686D
from 
    calc_data
where product = 'driveway' 
and type = 'residential'
group by 1
),
quote2win
as 
(
select 
    date_trunc(quoted_at,{period}) as date,
    sum(is_win)/nullif(sum(is_quoted),0) as SAL687,
    sum(is_quote14* is_won14q)/nullif(sum(is_quote14),0) as SAL688,
    sum(is_quote30* is_won30q)/nullif(sum(is_quote30),0) as SAL689,
    sum(is_quote60* is_won60q)/nullif(sum(is_quote60),0) as SAL690
from 
    calc_data 
group by 1
),
quote2winf
as 
(
select 
    date_trunc(quoted_at,{period}) as date,
    sum(is_win)/nullif(sum(is_quoted),0) as SAL687F,
    sum(is_quote14* is_won14q)/nullif(sum(is_quote14),0) as SAL688F,
    sum(is_quote30* is_won30q)/nullif(sum(is_quote30),0) as SAL689F,
    sum(is_quote60* is_won60q)/nullif(sum(is_quote60),0) as SAL690F
from 
    calc_data
where product = 'fence'
and type = 'residential'
group by 1
),
quote2wint
as 
(
select 
    date_trunc(quoted_at,{period}) as date,
    sum(is_win)/nullif(sum(is_quoted),0) as SAL687T,
    sum(is_quote14* is_won14q)/nullif(sum(is_quote14),0) as SAL688T,
    sum(is_quote30* is_won30q)/nullif(sum(is_quote30),0) as SAL689T,
    sum(is_quote60* is_won60q)/nullif(sum(is_quote60),0) as SAL690T
from 
    calc_data
where product = 'turf'
and type = 'residential'
group by 1
),
quote2wind
as 
(
select 
    date_trunc(quoted_at,{period}) as date,
    sum(is_win)/nullif(sum(is_quoted),0) as SAL687D,
    sum(is_quote14* is_won14q)/nullif(sum(is_quote14),0) as SAL688D,
    sum(is_quote30* is_won30q)/nullif(sum(is_quote30),0) as SAL689D,
    sum(is_quote60* is_won60q)/nullif(sum(is_quote60),0) as SAL690D
from 
    calc_data 
where product = 'driveway'
and type = 'residential' 
group by 1
)
select 
    *
from
    timeseries t 
    left join lead2quote using(date)
    left join lead2quotef using(date)
    left join lead2quote_t using(date)
    left join lead2quoted using(date)
    left join quote2win using(date)
    left join quote2winf using(date)
    left join quote2wint using(date)
    left join quote2wind using(date)
order by 1 desc
