with cos as (select c.id as contractor_id, u.full_name from ergeon.hrm_contractor c join ergeon.core_user u on u.id = c.user_id)
select 
  h.pallet__ as pallet_key, 
  h.contractor as full_name, 
  received_date , 
  delivered_date, 
  contractor_extended as revenue,
  home_depot_extended as cost,
  gap, 
  status,
  c.contractor_id
from 
  ext_marketing.pallet_tracking_history_sheet h  left join
  cos c on c.full_name = h.contractor
where
  c.contractor_id is not null
