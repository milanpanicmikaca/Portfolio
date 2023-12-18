with ranked_data as
(
SELECT 
    *, 
    row_number() over (partition by concat(id_lineitem, time_interval)) as rank 
FROM `bigquerydatabase-270315.cloud_costs.aws`
)
select * except(rank)
from ranked_data
where rank = 1