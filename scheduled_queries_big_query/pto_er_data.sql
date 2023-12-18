with manager_list as (
    select distinct
        cu . full_name as manager,
        m.id as manager_id,
        cu.email as manager_email
    from
        ergeon.hrm_staff s join
        ergeon.hrm_stafflog sl on sl.id = s.current_stafflog_id join
        ergeon.hrm_staff m on s.manager_id = m.id join
        ergeon.hrm_stafflog ml on ml.id = m.current_stafflog_id join
        ergeon.core_user cu on cu.id = m.user_id
    where
        s.is_staff and m.is_staff
        and sl.change_type <> 'left' and ml.change_type <> 'left'
),

hire_list as (
    select
        staff_id,
        effective_date as hire_date
    from
        ergeon.hrm_stafflog sl left join
        ergeon.hrm_staffposition p on p.id = sl.position_id
    where
        lower(internal_title) not like '%bootcamp%'
        and commitment <> 'pb'
    qualify rank() over(partition by staff_id order by sl.id) = 1
),

staff_list as (
    select
        ml.manager,
        u.full_name,
        hire_date as started_at,
        d.name as department,
        la.name as ladder,
        internal_title,
        s.id as staff_id,
        sl.contract,
        manager_email,
        u.email as staff_email,
        cast(hire_date + interval 6 month as date) as first_year_pto_at,
        cast(hire_date + interval 6 + 12 month as date) as second_year_pto_at,
        cast(hire_date + interval 6 + 12 + 12 month as date) as third_year_pto_at,
        cast(hire_date + interval 6 + 12 + 12 + 12 month as date) as fourth_year_pto_at,
        cast(hire_date + interval 6 + 12 + 12 + 12 + 12 month as date) as fifth_year_pto_at,
        cast(hire_date + interval 6 + 12 + 12 + 12 + 12 + 12 month as date) as sixth_year_pto_at,
        cast(hire_date as date) as first_year_er_at,
        cast(hire_date + interval 12 month as date) as second_year_er_at,
        cast(hire_date + interval 12 + 12 month as date) as third_year_er_at,
        cast(hire_date + interval 12 + 12 + 12 month as date) as fourth_year_er_at,
        cast(hire_date + interval 12 + 12 + 12 + 12 month as date) as fifth_year_er_at,
        cast(hire_date + interval 12 + 12 + 12 + 12 + 12 month as date) as sixth_year_er_at,
        cast(hire_date + interval 12 + 12 + 12 + 12 + 12 + 12 month as date) as seventh_year_er_at
    from
        manager_list ml join
        ergeon.hrm_staff s on s.manager_id = ml.manager_id join
        ergeon.core_user u on s.user_id = u.id join
        ergeon.hrm_stafflog sl on sl.id = s.current_stafflog_id join
        hire_list hl on hl.staff_id = s.id join
        ergeon.hrm_staffposition p on p.id = sl.position_id join
        ergeon.hrm_ladder la on la.id = p.ladder_id join
        ergeon.hrm_department d on d.id = la.department_id
    where
        s.is_staff
        and sl.change_type <> 'left'
),

pto_er_list as (
    select
        sl.staff_id,
        start_date,
        first_day_back,
        days_requested,
        reason as description,
        cast(approved_at as date) as approved_at,
        cast(paid_at as date) as paid_at,
        'PTO' as type,
        null as amount,
        null as receipt_at,
        null as reimbursement_type,
        case
            when start_date < first_year_pto_at then 'pre_eligibility_pto'
            when start_date between first_year_pto_at and second_year_pto_at then 'first_year_pto'
            when start_date > second_year_pto_at and start_date <= third_year_pto_at then 'second_year_pto'
            when start_date > third_year_pto_at and start_date <= fourth_year_pto_at then 'third_year_pto'
            when start_date > fourth_year_pto_at and start_date <= fifth_year_pto_at then 'fourth_year_pto'
            when start_date > fifth_year_pto_at and start_date <= sixth_year_pto_at then 'fifth_year_pto'
        end as pto_year,
        null as reimbursement_year,
        hs.id as pto_id,
        concat('https://api.ergeon.in/public-admin/hrm/stafftimeoffrequest/', hs.id, '/change/') as pto_link,
        null as reimbursement_id,
        null as reimbursement_category,
        null as reimbursement_link,
        coalesce(label, 'No Category') as pto_category
    from
        ergeon.hrm_stafftimeoffrequest hs left join
        ergeon.hrm_stafftimeoffcategory cat on hs.category_id = cat.id left join
        staff_list sl on sl.staff_id = hs.staff_id
    where
        cat.label <> 'Unpaid time off'
        and approved_at is not null
        and status_id = 48
        and hs.deleted_at is null
    union all
    select
        sl.staff_id,
        null as start_date,
        null as first_day_back,
        null as days_requested,
        reason as description,
        cast(approved_at as date) as approved_at,
        cast(paid_at as date) as paid_at,
        'reimbursement' as type,
        amount,
        receipt_at,
        case
            when label like 'Benefit:%' then 'benefits'
            when label = 'Laptop' then 'laptop'
            else 'other'
        end as reimbursement_type,
        null as pto_year,
        case
            when receipt_at <= second_year_er_at then 'first_year_reimbursement'
            when receipt_at > second_year_er_at and receipt_at <= third_year_er_at then 'second_year_reimbursement'
            when receipt_at > third_year_er_at and receipt_at <= fourth_year_er_at then 'third_year_reimbursement'
            when receipt_at > fourth_year_er_at and receipt_at <= fifth_year_er_at then 'fourth_year_reimbursement'
            when receipt_at > fifth_year_er_at and receipt_at <= sixth_year_er_at then 'fifth_year_reimbursement'
            when receipt_at > sixth_year_er_at and receipt_at <= seventh_year_er_at then 'sixth_year_reimbursement'
        end as reimbursement_year,
        null as pto_id,
        null as pto_link,
        hs.id as reimbursement_id,
        coalesce(label, 'No Category') as reimbursement_category,
        concat('https://api.ergeon.in/public-admin/hrm/staffexpensereimbursement/', hs.id, '/change/') as reimbursement_link,
        null as pto_category
    from
        ergeon.hrm_staffexpensereimbursement hs left join
        ergeon.hrm_staffexpensereimbursementcategory rec on rec.id = hs.category_id left join
        staff_list sl on sl.staff_id = hs.staff_id
    where
        approved_at is not null
        and hs.deleted_at is null
        and status_id = 54
)

select
    manager,
    full_name as freelancer,
    started_at,
    first_year_pto_at,
    second_year_pto_at,
    third_year_pto_at,
    fourth_year_pto_at,
    fifth_year_pto_at,
    first_year_er_at,
    second_year_er_at,
    third_year_er_at,
    fourth_year_er_at,
    fifth_year_er_at,
    sixth_year_er_at,
    department, ladder, internal_title as position, staff_id, contract, manager_email, staff_email,
    pto_er_list.* except(staff_id)
from
    staff_list left join
    pto_er_list using (staff_id)
