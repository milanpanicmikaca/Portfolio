with term1 as (
    select
        *,
        regexp_replace(attr_key, 'attr_', '') as my_attr_key
    from
        ergeon.calc_term
),

term2 as (
    select
        t.id,
        my_attr_key || ' ' || operator || ' (' || string_agg(distinct value_key, ', ' order by value_key) || ')' as description
    from
        ergeon.calc_termvalue tv left join
        term1 t on tv.term_id = t.id
    group by t.id, my_attr_key, operator
    order by 1, 2
),

my_term as (
    select t.*, my_attr_key, description from ergeon.calc_term t join term1 on term1.id = t.id join term2 on term2.id = t.id
),

rule1 as (
    select
        ifrt.rule_id as id,
        'if \n  ' || string_agg(
            distinct ift.description, ' and\n  ' order by ift.description
        ) || '\nthen\n  ' || string_agg(distinct thent.description, ' and\n  ' order by thent.description) as description,
        string_agg(distinct ift.my_attr_key, ', ' order by ift.my_attr_key) as if_attributes,
        string_agg(distinct thent.my_attr_key, ', ' order by thent.my_attr_key) as then_attributes
    from
        ergeon.calc_ruleifterm ifrt join
        my_term ift on ift.id = ifrt.term_id join
        ergeon.calc_rulethenterm thenrt on thenrt.rule_id = ifrt.rule_id join
        my_term thent on thent.id = thenrt.term_id
    group by 1
),

my_rule as (
    select
        r.* except (description),
        rule1.description,
        if_attributes,
        then_attributes,
        ty.item as type
    from
        rule1 join
        ergeon.calc_rule r on r.id = rule1.id join
        ergeon.product_catalogtype ty on ty.id = r.type_id
)

select
    id as rule_id,
    type,
    description,
    if_attributes,
    then_attributes,
    created_at,
    concat("https://api.ergeon.in/public-admin/calc/rule/", id, "/change/") as url
from my_rule
where id is not null
