select
    *
from int_data.escalation_query
where lower(core_issues_string) not like '%scop%'
