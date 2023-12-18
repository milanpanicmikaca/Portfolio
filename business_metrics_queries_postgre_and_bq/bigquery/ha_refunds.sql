-- upload to BQ
select 
    date_trunc(ha_refund_at, {period}) as date,
    sum(ha_initial_fee-ha_fee) as MAR112, --ha_fee without refund - ha_fee with refunds = ha_refund
    sum(case when product like '/Fence%' then ha_initial_fee-ha_fee else 0 end) as MAR112F,
    sum(case when product like '/Driveway%' then ha_initial_fee-ha_fee else 0 end) as MAR112D,
    sum(case when product like '%Artificial Grass%' then ha_initial_fee-ha_fee else 0 end) as MAR112T
from int_data.order_ue_materialized
where channel like '%Home Advisor%' and ha_refund_at is not null
group by 1
order by 1 desc
