select
    current_date('America/Los_Angeles') as date,
    cc.contractor_id,
    cc.id as crew_id,
    sum(cc.avg_projects_per_week * extract(day from last_day(current_date('America/Los_Angeles'))) / 7) as capacity --weeks per month
from ergeon.contractor_contractorcrew cc
where avg_projects_per_week is not null
group by 1, 2, 3
