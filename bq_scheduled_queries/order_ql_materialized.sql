select
    ql.*,
    ue.created_at, ue.cancelled_at, ue.quoted_at, ue.won_at, ue.completed_at,
    lead_id, is_lead, type, product, product_quoted, tier, channel1, channel,
    sales_rep, project_manager, quoter, photographer, contractor,
    has_escalation, multi_party_approval, order_status, lost_reason,
    market, region, state, ue.segment, segment_l1, msa, county, old_region, is_draft_editor, quoted_dep
from
    int_data.catalog_groupedquoteline ql left join
    int_data.order_ue_materialized ue using (order_id)
