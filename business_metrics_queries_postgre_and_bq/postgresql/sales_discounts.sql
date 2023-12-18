-- upload to BQ
with
calc_sales_discount as
(
        select
                  ql.quote_id,
                   sum(ql.price) as sales_discount
        from quote_quoteline ql
        left join product_catalog pg on pg.id = ql.catalog_id
        left join product_catalogtype pct on pct.id = pg.type_id
         where
                 pct.category = 'sales_discount'
         group by 1
),
calc_all_approved_quotes as
(
        select
                  rank() over (partition by order_id order by approved_at),
            *
         from quote_quote
          where
                   approved_at is not null
                 and approved_at >= '2018-04-16'
),
calc_first_approved_quotes as
(
        select
            q.id,
            q.order_id,
            q.created_at,
            q.approved_at,
            o.product_id,
            q.total_price,
            case when o.product_id = 105 then 1 else 0 end as is_fence,
            case when o.product_id = 34 then 1 else 0 end as is_driveway,
            case when o.product_id = 132 then 1 else 0 end as is_turf,
            case when o.product_id = 105 then q.total_price else 0 end as is_fence_revenue,
            case when o.product_id = 132 then q.total_price else 0 end as is_turf_revenue,
            case when o.product_id = 34 then q.total_price else 0 end as is_driveway_revenue,
            case when o.product_id = 105 then q.total_cost else 0 end as is_fence_cost,
            case when o.product_id = 34 then q.total_cost else 0 end as is_driveway_cost,
            case when o.product_id = 132 then q.total_cost else 0 end as is_turf_cost,
                   abs(csd.sales_discount) as sales_discount,
             case when o.product_id = 105 then abs(csd.sales_discount) else 0 end as is_fence_sales_discount,
             case when o.product_id = 132 then abs(csd.sales_discount) else 0 end as is_turf_sales_discount,
             case when o.product_id = 34 then abs(csd.sales_discount) else 0 end as is_driveway_sales_discount,
               is_commercial::integer
        from calc_all_approved_quotes q
          left join store_order o on o.id = q.order_id
          left join calc_sales_discount csd on csd.quote_id = q.id
          left join core_lead l on l.order_id = o.id
          left join core_house ch on ch.id = o.house_id
                  left join customers_customer cc on cc.id = ch.customer_id
          where
                  rank = 1
                  and o.id not in (50815,56487,59225,59348,59404,59666,59670,59743,59753,59789,59805,59813,59878,59908,59922,60273,60283,60401,60547,60589,60590,60595,60596,60597,60612,69998)
)
select
        date_trunc('{period}',approved_at at time zone 'America/Los_Angeles')::date as date,
        coalesce(sum(sales_discount)/nullif(sum(total_price),0),0) as SAL231, --sales_discount_perc
        coalesce(sum(case when is_commercial = 0 then is_fence_sales_discount else 0 end)/nullif(sum(case when is_commercial = 0 then is_fence_revenue else 0 end),0),0) as SAL231F, --fence_sales_discount_perc
        coalesce(sum(case when is_commercial = 0 then is_turf_sales_discount else 0 end)/nullif(sum(case when is_commercial = 0 then is_turf_revenue else 0 end),0),0) as SAL231T, --turf_sales_discount_perc
        coalesce(sum(case when is_commercial = 0 then is_driveway_sales_discount else 0 end)/nullif(sum(case when is_commercial = 0 then is_driveway_revenue else 0 end),0),0) as SAL231D --driveway_sales_discount_perc
from calc_first_approved_quotes
group by 1
order by 1 desc
