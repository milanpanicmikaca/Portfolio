-- upload to BQ
select 
  date_trunc(created_at,{period}) as date,
  sum(yelp_cpl_budget) as MAR133,-- yelp_spend
  sum(case when product like '/Fence%' then yelp_cpl_budget else 0 end) as MAR133F,-- yelp_spend_fence
  sum(case when product like '/Driveway%' then yelp_cpl_budget else 0 end) as MAR133D, -- yelp_spend_driveway
  sum(case when product like '/Landscaping Design%' then yelp_cpl_budget else 0 end) as MAR133T --yelp_spend_turf
from int_data.order_ue_materialized
where channel like '%Yelp%'
group by 1
order by 1 desc
