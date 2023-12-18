 select --reviews
    date(rc.date) as date,
    u.full_name,
    u.email,
    rc.review_id as id,
    CONCAT('https://api.ergeon.in/public-admin/feedback/review/',rc.review_id,'/change/') as url,
    rc.description,
    rc.type as bonus_type,
    rc.amount,
  from compensation_system.sales_review_commissions_trx rc
    left join ergeon.hrm_staff s on s.id = rc.staff_id
    left join ergeon.core_user u on u.id = s.user_id
union all
  select --escalations
    date(ec.date) as date,
    u.full_name,
    u.email,
    ec.escalation_id as id,
    CONCAT('https://api.ergeon.in/public-admin/store/escalation/',ec.escalation_id,'/change/') as url,
    ec.description,
    ec.type as bonus_type,
    ec.amount,
  from compensation_system.sales_escalation_commissions_trx ec
    left join ergeon.hrm_staff s on s.id = ec.staff_id
    left join ergeon.core_user u on u.id = s.user_id
union all
  select --adjustments
    date(a.date) as date,
    u.full_name,
    u.email,
    a.order_id as id,
    CONCAT('https://admin.ergeon.in/quoting-tool/',a.order_id,'/overview/') as url,
    a.description,
    a.type as bonus_type,
    a.amount,
  from compensation_system.review_adjustments a
    left join ergeon.core_user u using (email)
    left join ergeon.hrm_staff s on s.user_id = u.id
union all
  select --adjustments
    date(a.date) as date,
    u.full_name,
    u.email,
    a.order_id as id,
    CONCAT('https://admin.ergeon.in/quoting-tool/',a.order_id,'/overview/') as url,
    a.description,
    a.type as bonus_type,
    a.amount,
  from compensation_system.commission_adjustments a
    left join ergeon.core_user u using (email)
    left join ergeon.hrm_staff s on s.user_id = u.id
