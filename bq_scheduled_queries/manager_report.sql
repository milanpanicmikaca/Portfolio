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
        case
            when sl.contract = 'hourly' then case
                when hire_date <= current_date() - interval 102 month then 28
                when hire_date <= current_date() - interval 90 month then 26
                when hire_date <= current_date() - interval 78 month then 24
                when hire_date <= current_date() - interval 66 month then 22
                when hire_date <= current_date() - interval 54 month then 20
                when hire_date <= current_date() - interval 42 month then 18
                when hire_date <= current_date() - interval 30 month then 16
                when hire_date <= current_date() - interval 18 month then 14
                when hire_date <= current_date() - interval 6 month then 12
                else 0
                end
        end as pto_days_available,
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

pto_days as (
    select
        sl.staff_id,
        sum(
            case
                when
                    cat.label <> 'Birthday' and start_date < first_year_pto_at then days_requested
                when cat.label = 'Birthday' and days_requested > 1 and start_date < first_year_pto_at then days_requested - 1 else 0
            end
        ) as pre_eligibility_pto,
        sum(
            case
                when
                    cat.label <> 'Birthday' and start_date between first_year_pto_at and second_year_pto_at then days_requested
                when
                    cat.label = 'Birthday' and days_requested > 1 and start_date between first_year_pto_at and second_year_pto_at then days_requested - 1
                else 0
            end
        ) as first_year_pto,
        sum(
            case
                when
                    cat.label <> 'Birthday' and start_date > second_year_pto_at and start_date <= third_year_pto_at then days_requested
                when
                    cat.label = 'Birthday' and days_requested > 1 and start_date > second_year_pto_at and start_date <= third_year_pto_at then days_requested - 1
                else 0
            end
        ) as second_year_pto,
        sum(
            case
                when
                    cat.label <> 'Birthday' and start_date > third_year_pto_at and start_date <= fourth_year_pto_at then days_requested
                when
                    cat.label = 'Birthday' and days_requested > 1 and start_date > third_year_pto_at and start_date <= fourth_year_pto_at then days_requested - 1
                else 0
            end
        ) as third_year_pto,
        sum(
            case
                when
                    cat.label <> 'Birthday' and start_date > fourth_year_pto_at and start_date <= fifth_year_pto_at then days_requested
                when
                    cat.label = 'Birthday' and days_requested > 1 and start_date > fourth_year_pto_at and start_date <= fifth_year_pto_at then days_requested - 1
                else 0
            end
        ) as fourth_year_pto,
        sum(
            case
                when
                    cat.label <> 'Birthday' and start_date > fifth_year_pto_at and start_date <= sixth_year_pto_at then days_requested
                when
                    cat.label = 'Birthday' and days_requested > 1 and start_date > fifth_year_pto_at and start_date <= sixth_year_pto_at then days_requested - 1
                else 0
            end
        ) as fifth_year_pto
    from
        ergeon.hrm_stafftimeoffrequest hs left join
        ergeon.hrm_stafftimeoffcategory cat on hs.category_id = cat.id left join
        staff_list sl on sl.staff_id = hs.staff_id
    where
        cat.label <> 'Unpaid time off'
        and approved_at is not null
        and status_id = 48
        and hs.deleted_at is null
    group by 1
),

exp_er as (
    select
        er.staff_id,
        sum(case when label like 'Benefit:%' and receipt_at <= second_year_er_at then amount else 0 end) as first_year_benefits,
        sum(
            case when label like 'Benefit:%' and receipt_at > second_year_er_at and receipt_at <= third_year_er_at then amount else 0 end
        ) as second_year_benefits,
        sum(
            case when label like 'Benefit:%' and receipt_at > third_year_er_at and receipt_at <= fourth_year_er_at then amount else 0 end
        ) as third_year_benefits,
        sum(
            case when label like 'Benefit:%' and receipt_at > fourth_year_er_at and receipt_at <= fifth_year_er_at then amount else 0 end
        ) as fourth_year_benefits,
        sum(
            case when label like 'Benefit:%' and receipt_at > fifth_year_er_at and receipt_at <= sixth_year_er_at then amount else 0 end
        ) as fifth_year_benefits,
        sum(
            case when label like 'Benefit:%' and receipt_at > sixth_year_er_at and receipt_at <= seventh_year_er_at then amount else 0 end
        ) as sixth_year_benefits,
        sum(case when label = 'Laptop' and receipt_at <= second_year_er_at then amount else 0 end) as first_year_laptop,
        sum(
            case when label = 'Laptop' and receipt_at >= second_year_er_at and receipt_at <= third_year_er_at then amount else 0 end
        ) as second_year_laptop,
        sum(
            case when label = 'Laptop' and receipt_at >= third_year_er_at and receipt_at <= fourth_year_er_at then amount else 0 end
        ) as third_year_laptop,
        sum(
            case when label = 'Laptop' and receipt_at >= fourth_year_er_at and receipt_at <= fifth_year_er_at then amount else 0 end
        ) as fourth_year_laptop,
        sum(
            case when label = 'Laptop' and receipt_at >= fifth_year_er_at and receipt_at <= sixth_year_er_at then amount else 0 end
        ) as fifth_year_laptop,
        sum(
            case when label = 'Laptop' and receipt_at >= sixth_year_er_at and receipt_at <= seventh_year_er_at then amount else 0 end
        ) as sixth_year_laptop,
        sum(
            case
                when label not like 'Benefit:%' and label <> 'Laptop' and receipt_at between first_year_er_at and second_year_er_at then amount else 0
            end
        ) as first_year_other,
        sum(
            case
                when
                    label not like 'Benefit:%' and label <> 'Laptop' and receipt_at >= second_year_er_at and receipt_at <= third_year_er_at then amount
                else 0
            end
        ) as second_year_other,
        sum(
            case
                when
                    label not like 'Benefit:%' and label <> 'Laptop' and receipt_at >= third_year_er_at and receipt_at <= fourth_year_er_at then amount
                else 0
            end
        ) as third_year_other,
        sum(
            case
                when
                    label not like 'Benefit:%' and label <> 'Laptop' and receipt_at >= fourth_year_er_at and receipt_at <= fifth_year_er_at then amount
                else 0
            end
        ) as fourth_year_other,
        sum(
            case
                when
                    label not like 'Benefit:%' and label <> 'Laptop' and receipt_at >= fifth_year_er_at and receipt_at <= sixth_year_er_at then amount
                else 0
            end
        ) as fifth_year_other,
        sum(
            case
                when
                    label not like 'Benefit:%' and label <> 'Laptop' and receipt_at >= sixth_year_er_at and receipt_at <= seventh_year_er_at then amount
                else 0
            end
        ) as sixth_year_other
    from
        ergeon.hrm_staffexpensereimbursement er left join
        ergeon.hrm_staffexpensereimbursementcategory rec on rec.id = er.category_id left join
        staff_list sl on sl.staff_id = er.staff_id
    where
        approved_at is not null
        and er.deleted_at is null
        and status_id = 54
    group by 1
),

final_data as (
    select
        manager,
        full_name as freelancer,
        started_at, coalesce(pre_eligibility_pto, 0) as pre_eligibility_pto,
        first_year_pto_at, case when first_year_pto_at <= current_date then coalesce(first_year_pto, 0) else null end as first_year_pto,
        second_year_pto_at, case when second_year_pto_at <= current_date then coalesce(second_year_pto, 0) else null end as second_year_pto,
        third_year_pto_at, case when third_year_pto_at <= current_date then coalesce(third_year_pto, 0) else null end as third_year_pto,
        fourth_year_pto_at, case when fourth_year_pto_at <= current_date then coalesce(fourth_year_pto, 0) else null end as fourth_year_pto,
        fifth_year_pto_at, case when fifth_year_pto_at <= current_date then coalesce(fifth_year_pto, 0) else null end as fifth_year_pto,
        first_year_er_at, case when first_year_er_at <= current_date then coalesce(first_year_benefits, 0) else null end as first_year_benefits,
        case when first_year_er_at <= current_date then coalesce(first_year_laptop, 0) else null end as first_year_laptop,
        case when first_year_er_at <= current_date then coalesce(first_year_other, 0) else null end as first_year_other,
        second_year_er_at, case when second_year_er_at <= current_date then coalesce(second_year_benefits, 0) else null end as second_year_benefits,
        case when second_year_er_at <= current_date then coalesce(second_year_laptop, 0) else null end as second_year_laptop,
        case when second_year_er_at <= current_date then coalesce(second_year_other, 0) else null end as second_year_other,
        third_year_er_at, case when third_year_er_at <= current_date then coalesce(third_year_benefits, 0) else null end as third_year_benefits,
        case when third_year_er_at <= current_date then coalesce(third_year_laptop, 0) else null end as third_year_laptop,
        case when third_year_er_at <= current_date then coalesce(third_year_other, 0) else null end as third_year_other,
        fourth_year_er_at, case when fourth_year_er_at <= current_date then coalesce(fourth_year_benefits, 0) else null end as fourth_year_benefits,
        case when fourth_year_er_at <= current_date then coalesce(fourth_year_laptop, 0) else null end as fourth_year_laptop,
        case when fourth_year_er_at <= current_date then coalesce(fourth_year_other, 0) else null end as fourth_year_other,
        fifth_year_er_at, case when fifth_year_er_at <= current_date then coalesce(fifth_year_benefits, 0) else null end as fifth_year_benefits,
        case when fifth_year_er_at <= current_date then coalesce(fifth_year_laptop, 0) else null end as fifth_year_laptop,
        case when fifth_year_er_at <= current_date then coalesce(fifth_year_other, 0) else null end as fifth_year_other,
        sixth_year_er_at, case when sixth_year_er_at <= current_date then coalesce(sixth_year_benefits, 0) else null end as sixth_year_benefits,
        case when sixth_year_er_at <= current_date then coalesce(sixth_year_laptop, 0) else null end as sixth_year_laptop,
        case when sixth_year_er_at <= current_date then coalesce(sixth_year_other, 0) else null end as sixth_year_other,
        department, ladder, internal_title as position, staff_id, contract, manager_email, staff_email, pto_days_available,
        case
            when first_year_pto_at > current_date then first_year_pto_at
            when second_year_pto_at > current_date then second_year_pto_at
            when third_year_pto_at > current_date then third_year_pto_at
            when fourth_year_pto_at > current_date then fourth_year_pto_at
            when fifth_year_pto_at > current_date then fifth_year_pto_at
            when sixth_year_pto_at > current_date then sixth_year_pto_at
        end as pto_renewal_date
    from
        staff_list left join
        pto_days using (staff_id) left join
        exp_er using (staff_id)
)

select
    *,
    coalesce(fifth_year_pto, fourth_year_pto, third_year_pto, second_year_pto, first_year_pto) as current_year_pto,
    case
        when fifth_year_pto is not null then fourth_year_pto
        when fourth_year_pto is not null then third_year_pto
        when third_year_pto is not null then second_year_pto
        when second_year_pto is not null then first_year_pto
    end as previous_year_pto,
    coalesce(
        sixth_year_benefits, fifth_year_benefits, fourth_year_benefits, third_year_benefits, second_year_benefits, first_year_benefits
    ) as current_year_benefits,
    coalesce(
        sixth_year_laptop, fifth_year_laptop, fourth_year_laptop, third_year_laptop, second_year_laptop, first_year_laptop
    ) as current_year_laptop,
    coalesce(sixth_year_other, fifth_year_other, fourth_year_other, third_year_other, second_year_other, first_year_other) as current_year_other,
    case
        when sixth_year_benefits is not null then fifth_year_benefits
        when fifth_year_benefits is not null then fourth_year_benefits
        when fourth_year_benefits is not null then third_year_benefits
        when third_year_benefits is not null then second_year_benefits
        when second_year_benefits is not null then first_year_benefits
    end as previous_year_benefits,
    case
        when sixth_year_laptop is not null then fifth_year_laptop
        when fifth_year_laptop is not null then fourth_year_laptop
        when fourth_year_laptop is not null then third_year_laptop
        when third_year_laptop is not null then second_year_laptop
        when second_year_laptop is not null then first_year_laptop
    end as previous_year_laptop,
    case
        when sixth_year_other is not null then fifth_year_other
        when fifth_year_other is not null then fourth_year_other
        when fourth_year_other is not null then third_year_other
        when third_year_other is not null then second_year_other
        when second_year_other is not null then first_year_other
    end as previous_year_other
from
    final_data
