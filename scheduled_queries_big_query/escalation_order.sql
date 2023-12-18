with gates as (
    select
        ql.order_id,
        quote_id,
        count(*) as gate_count
    from
        int_data.order_ue_materialized ue left join
        int_data.order_ql_materialized ql on ql.quote_id = ue.first_approved_quote_id
    where
        item_type in (
            'fence-gate',
            'fence-gate-custom',
            'chain_link_gate',
            'cl-custom-gate',
            'vinyl_gate_type',
            'vinyl-custom-gate',
            'bw-fence-gate',
            'bw-custom-gate'
        )
        and ue.won_at is not null
    group by 1, 2
),

catalog_raw_data as ( --CTE has duplicates
    select
        ue.order_id,
        ql.quote_id,
        quoteline_id,
        frame_style,
        finish_height,
        picket_material,
        picket_size,
        picket_build,
        concat(post_material, ', ', post_size) as post_type,
        rails_material1 as rails_material,
        rails,
        chain_link_fence,
        vinyl_base_style,
        vinyl_finish_height,
        vinyl_color,
        coalesce(gate_count, 0) as gate_count,
        ue.has_escalation,
        ue.project_manager,
        ue.contractor,
        ue.completed_at,
        ue.region,
        ue.market,
        ue.county,
        ue.city,
        ue.state,
        ue.onsite_type,
        ue.product_quoted,
        ue.quoter,
        ue.multi_party_approval,
        eq.concatenated_teams_array_distinct,
        eq.core_issues,
        eq.escalation_status,
        eq.sales_rep,
        (
            last_approved_mktg_discount + last_approved_sales_discount + last_approved_delivery_discount + (
                contractor_pay - last_approved_cost
            ) + materials_pay + cost_of_sales + last_approved_pricing_discount + finance_disc
        ) as discount,
        ue.revenue,
        change_order_count,
        concat(ue.project_manager, ' - ', ue.contractor) as combo_pm_contractor,
        concat(ue.contractor, ' - ', ue.market) as combo_contractor_market,
        rank() over (partition by ue.order_id order by eq.escalation_id) as rank --one order_id can have multiple escalations
    from
        int_data.order_ue_materialized ue left join
        int_data.order_ql_materialized ql on ql.quote_id = ue.first_approved_quote_id left join
        gates g on g.order_id = ue.order_id left join
        int_data.escalation_query eq on ue.order_id = eq.order_id
    where
        ql_length <> 0
        and ql_length is not null
        and first_approved_quote_id is not null
    --and change_order_count = 0
    qualify rank() over (partition by quote_id order by ql_length desc, quoteline_id) = 1
)

select
    * except (rank)
from catalog_raw_data where rank = 1
