-- upload to BQ
with
    leakage_ue
as
(
select 
  ue.order_id,
  ue.completed_at,
  last_approved_mktg_discount as mktg,
  last_approved_sales_discount as sales,
  last_approved_delivery_discount as delivery,
  finance_disc as finance,
  contractor_pay - last_approved_cost + wwo_installer_leakage as installer,
  cost_of_sales,
  materials_pay as materials,
  last_approved_pricing_discount as pricing,
  revenue as revenue,
  delta,
  contractor_pay,
  ue.product_quoted
from int_data.order_ue_materialized ue
where ue.is_warranty_order is false
and ue.completed_at is not null
),
margin_leakage
as
(
    select
        order_id,
        completed_at as date,
        (mktg + sales + delivery + finance + installer + cost_of_sales + materials + pricing + delta)/nullif(revenue,0) as margin_leak,
    from leakage_ue
),
margin_classification as
(
select
        order_id, 
        case when margin_leak <= 0 then 'no_margin_leakage'
                when margin_leak > 0 and margin_leak <= 0.01 then 'margin_leakage_0_1'
                when margin_leak > 0.01 and margin_leak <= 0.02 then 'margin_leakage_1_2'
                when margin_leak > 0.02 and margin_leak <= 0.03 then 'margin_leakage_2_3'
                when margin_leak > 0.03 then 'margin_leakage_3' end as margin_class
from margin_leakage
)
select
        date_trunc(date, {period}) as date,
        --summary
        sum(mktg + sales + delivery + finance + installer + cost_of_sales + materials + pricing + delta)/sum(nullif(revenue,0)) as PRO123,
        countif(margin_class = 'no_margin_leakage')/nullif(cast(count(ml.order_id) as decimal),0) as PRO124,
        countif(margin_class = 'margin_leakage_0_1')/nullif(cast(count(ml.order_id) as decimal),0) as PRO125,
        countif(margin_class = 'margin_leakage_1_2')/nullif(cast(count(ml.order_id) as decimal),0) as PRO126,
        countif(margin_class = 'margin_leakage_2_3')/nullif(cast(count(ml.order_id) as decimal),0) as PRO127,
        countif(margin_class = 'margin_leakage_3')/nullif(cast(count(ml.order_id) as decimal),0) as PRO128,
        sum(mktg)/sum(nullif(revenue,0)) as PRO141,
        sum(sales)/sum(nullif(revenue,0)) as PRO142,
        sum(delivery)/sum(nullif(revenue,0)) as PRO143,
        sum(installer)/sum(nullif(revenue,0)) as PRO144,
        sum(finance)/sum(nullif(revenue,0)) as PRO145,
        sum(materials)/sum(nullif(revenue,0)) as PRO146,
        sum(pricing)/sum(nullif(revenue,0)) as PRO147,
        sum(cost_of_sales)/sum(nullif(revenue,0)) as PRO150,
        sum(delta)/sum(nullif(revenue,0)) as PRO152,
        --fence
        sum(case when product_quoted like '%/Fence%/%' then (mktg + sales + delivery + finance + installer + cost_of_sales + materials + pricing + delta) else 0 end) / nullif(sum(case when product_quoted like '%/Fence%/%' then revenue else 0 end),0) as PRO123F,
        countif(margin_class = 'no_margin_leakage' and product_quoted like '%/Fence%/%')/nullif(cast(count(case when product_quoted like '%/Fence%/%' then ml.order_id else 0 end) as decimal),0) as PRO124F,
        countif(margin_class = 'margin_leakage_0_1' and product_quoted like '%/Fence%/%')/nullif(cast(count(case when product_quoted like '%/Fence%/%' then ml.order_id else 0 end) as decimal),0) as PRO125F,
        countif(margin_class = 'margin_leakage_1_2' and product_quoted like '%/Fence%/%')/nullif(cast(count(case when product_quoted like '%/Fence%/%' then ml.order_id else 0 end) as decimal),0) as PRO126F,
        countif(margin_class = 'margin_leakage_2_3' and product_quoted like '%/Fence%/%')/nullif(cast(count(case when product_quoted like '%/Fence%/%' then ml.order_id else 0 end) as decimal),0) as PRO127F,
        countif(margin_class = 'margin_leakage_3' and product_quoted like '%/Fence%/%')/nullif(cast(count(case when product_quoted like '%/Fence%/%' then ml.order_id else 0 end) as decimal),0) as PRO128F,
        sum (case when product_quoted like '%/Fence%/%' then mktg else 0 end)/nullif(sum(case when product_quoted like '%/Fence%/%' then revenue else 0 end),0) as PRO141F,
        sum (case when product_quoted like '%/Fence%/%' then sales else 0 end)/nullif(sum(case when product_quoted like '%/Fence%/%' then revenue else 0 end),0) as PRO142F,
        sum (case when product_quoted like '%/Fence%/%' then delivery else 0 end)/nullif(sum(case when product_quoted like '%/Fence%/%' then revenue else 0 end),0) as PRO143F,
        sum (case when product_quoted like '%/Fence%/%' then installer else 0 end)/nullif(sum(case when product_quoted like '%/Fence%/%' then revenue else 0 end),0) as PRO144F,
        sum (case when product_quoted like '%/Fence%/%' then finance else 0 end)/nullif(sum(case when product_quoted like '%/Fence%/%' then revenue else 0 end),0) as PRO145F,
        sum (case when product_quoted like '%/Fence%/%' then materials else 0 end)/nullif(sum(case when product_quoted like '%/Fence%/%' then revenue else 0 end),0) as PRO146F,
        sum (case when product_quoted like '%/Fence%/%' then pricing else 0 end)/nullif(sum(case when product_quoted like '%/Fence%/%' then revenue else 0 end),0) as PRO147F,
        sum (case when product_quoted like '%/Fence%/%' then cost_of_sales else 0 end)/nullif(sum(case when product_quoted like '%/Fence%/%' then revenue else 0 end),0) as PRO150F,
        sum (case when product_quoted like '%/Fence%/%' then delta else 0 end)/nullif(sum(case when product_quoted like '%/Fence%/%' then revenue else 0 end),0) as PRO152F,
        --turf
        sum(case when product_quoted like '%Artificial Grass%' then (mktg + sales + delivery + finance + installer + cost_of_sales + materials + pricing + delta) else 0 end) / nullif(sum(case when product_quoted like '%Artificial Grass%' then revenue else 0 end),0) as PRO123T,
        countif(margin_class = 'no_margin_leakage' and product_quoted like '%Artificial Grass%')/nullif(cast(count(case when product_quoted like '%Artificial Grass%' then ml.order_id else 0 end) as decimal),0) as PRO124T,
        countif(margin_class = 'margin_leakage_0_1' and product_quoted like '%Artificial Grass%')/nullif(cast(count(case when product_quoted like '%Artificial Grass%' then ml.order_id else 0 end) as decimal),0) as PRO125T,
        countif(margin_class = 'margin_leakage_1_2' and product_quoted like '%Artificial Grass%')/nullif(cast(count(case when product_quoted like '%Artificial Grass%' then ml.order_id else 0 end) as decimal),0) as PRO126T,
        countif(margin_class = 'margin_leakage_2_3' and product_quoted like '%Artificial Grass%')/nullif(cast(count(case when product_quoted like '%Artificial Grass%' then ml.order_id else 0 end) as decimal),0) as PRO127T,
        countif(margin_class = 'margin_leakage_3' and product_quoted like '%Artificial Grass%')/nullif(cast(count(case when product_quoted like '%Artificial Grass%' then ml.order_id else 0 end) as decimal),0) as PRO128T,
        sum (case when product_quoted like '%Artificial Grass%' then mktg else 0 end)/nullif(sum(case when product_quoted like '%Artificial Grass%' then revenue else 0 end),0) as PRO141T,
        sum (case when product_quoted like '%Artificial Grass%' then sales else 0 end)/nullif(sum(case when product_quoted like '%Artificial Grass%' then revenue else 0 end),0) as PRO142T,
        sum (case when product_quoted like '%Artificial Grass%' then delivery else 0 end)/nullif(sum(case when product_quoted like '%Artificial Grass%' then revenue else 0 end),0) as PRO143T,
        sum (case when product_quoted like '%Artificial Grass%' then installer else 0 end)/nullif(sum(case when product_quoted like '%Artificial Grass%' then revenue else 0 end),0) as PRO144T,
        sum (case when product_quoted like '%Artificial Grass%' then finance else 0 end)/nullif(sum(case when product_quoted like '%Artificial Grass%' then revenue else 0 end),0) as PRO145T,
        sum (case when product_quoted like '%Artificial Grass%' then materials else 0 end)/nullif(sum(case when product_quoted like '%Artificial Grass%' then revenue else 0 end),0) as PRO146T,
        sum (case when product_quoted like '%Artificial Grass%' then pricing else 0 end)/nullif(sum(case when product_quoted like '%Artificial Grass%' then revenue else 0 end),0) as PRO147T,
        sum (case when product_quoted like '%Artificial Grass%' then cost_of_sales else 0 end)/nullif(sum(case when product_quoted like '%Artificial Grass%' then revenue else 0 end),0) as PRO150T,
        sum (case when product_quoted like '%Artificial Grass%' then delta else 0 end)/nullif(sum(case when product_quoted like '%Artificial Grass%' then revenue else 0 end),0) as PRO152T,
from margin_leakage ml
left join margin_classification mc on mc.order_id = ml.order_id
left join leakage_ue lue on lue.order_id = ml.order_id
group by 1
