  select --75% Bonus
    date(wc.date) as date,
    u.full_name,
    u.email,
    wc.order_id as id,
    CONCAT('https://admin.ergeon.in/quoting-tool/',wc.order_id,'/overview/') as url,
    wc.description,
    wc.type as bonus_type,
    wc.amount,
  from compensation_system.sales_won_commissions_trx wc
    left join ergeon.hrm_staff s on s.id = wc.staff_id
    left join ergeon.core_user u on u.id = s.user_id
union all
  select --25% Bonus
    date(cc.date) as date,
    u.full_name,
    u.email,
    cc.order_id as id,
    CONCAT('https://admin.ergeon.in/quoting-tool/',cc.order_id,'/overview/') as url,
    cc.description,
    cc.type as bonus_type,
    cc.amount,
  from compensation_system.sales_completed_commissions_trx cc
    left join ergeon.hrm_staff s on s.id = cc.staff_id
    left join ergeon.core_user u on u.id = s.user_id
union all
  select --staining
    date(sc.date) as date,
    u.full_name,
    u.email,
    sc.order_id as id,
    CONCAT('https://admin.ergeon.in/quoting-tool/',sc.order_id,'/overview/') as url,
    sc.description,
    sc.type as bonus_type,
    sc.amount,
  from compensation_system.sales_stainingupsell_commissions_trx sc
    left join ergeon.hrm_staff s on s.id = sc.staff_id
    left join ergeon.core_user u on u.id = s.user_id
