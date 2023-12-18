Select 
  cc.id as customer_id,
  gid,
  is_commercial,
  is_key_account,
  email,
  cu.full_name as customer,
  orders,
  quotes,
  wins,
  completed,
  market,
  region,
  old_region,
  channel1 as customer_channel,
  segment,
  product,
  product_quoted,
  customer_arrival_at,
  mktg_fee,
  revenue
from 
  ergeon.customers_customer cc left join
  ergeon.customers_contact co on co.id = contact_id left join 
  ergeon.core_user cu on cu.id = co.user_id left join
  int_data.customer_ue_materialized ue on ue.id = cc.id
