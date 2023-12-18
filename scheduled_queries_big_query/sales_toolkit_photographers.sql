with regions_zip as (
    select 
        id,
        name,
        replace(zipcode, '"', '') as zipcode
    from ergeon.schedule_salesregion r, unnest(zip_codes) as zipcode
    where name not like "%(Unused)%"
    and name not like "%(Hold)%"
), photographers_regions as (
    select 
    sa.sales_region_id as id,
    u.full_name,
from ergeon.schedule_availability sa
  left join ergeon.hrm_staff s on s.id = sa.employee_id
  left join ergeon.core_user u on u.id = s.user_id
where date_trunc(date,week) = date_trunc(current_date(),week)
), final as (
select distinct
    pr.full_name as photographer,
    c.id as city_id,
    c.name as city,
    co.name as county,
from regions_zip z 
left join photographers_regions pr using (id)
left join ergeon.geo_zipcode gz on gz.code = z.zipcode
left join ergeon.geo_city c on c.id = gz.city_id
left join ergeon.geo_county co on co.id = gz.county_id
where full_name is not null
)
select 
    city_id,
    city,
    case when county not like '%County' then null else LEFT(county, length(county) - 7) end as county,
    string_agg(photographer, ', ') as photographers
from final f
group by 1, 2, 3