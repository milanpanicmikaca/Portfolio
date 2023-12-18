with msa_data as (
    select
        mv.date + 1 as date,
        hm.ha_msa_name,
        case
            when lower(ha_msa_name) like '%texarkana%' then 'TX'
            when lower(ha_msa_name) like '%other%' then left(ha_msa_name, 2)
            when right(ha_msa_name, 6) like '%-%-%' then left(right(ha_msa_name, 8), 2)
            when right(ha_msa_name, 4) like '%-%' then left(right(ha_msa_name, 5), 2)
            else right(ha_msa_name, 2)
        end as ha_state_name,
        em.id as ergeon_msa_id,
        case
            when em.name is null then 'Unknown'
            else em.name
        end as ergeon_msa_name,
        ht.ha_task_name,
        ps.id as ergeon_task_id,
        case
            when ps.label is null then 'Unknown'
            else ps.label
        end as ergeon_task_name,
        case
            when sp.name is null then 'Unknown'
            else sp.name
        end as ergeon_product,
        pm.name as ergeon_market_name,
        pm.code as ergeon_market_code,
        case
            when pm.id in (2, 10, 9, 3, 29, 4, 31, 30, 8, 13) then 'North California'
            when pm.id in (6, 5, 14, 7, 1, 12, 11) then 'South California'
            when pm.code like '%-TX-%' then 'Texas'
            when pm.code like '%-GA-%' then 'Georgia'
            when pm.code like '%-MD-%' then 'Maryland'
            when pm.code like '%-PA-%' then 'Pennsylvania'
            when pm.code like '%-VA-%' then 'Virginia'
            when pm.code like '%-FL-%' then 'Florida'
            when pm.code like '%-WA-%' then 'Washington'
            when pm.code is null then 'Unknown'
            else 'Other'
        end as ergeon_old_region_name,
        case
            when pr.name is null then 'Unknown'
            else pr.name
        end as ergeon_region_name,
        case
            when
                ht.ha_task_name like '%Chain Link%' and ht.ha_task_name not like '%Repair%'
                and ht.ha_task_name not like '%Business%' then 'Chain Link'
            when ht.ha_task_name like '%Wood%' and ht.ha_task_name not like '%Business%' then 'Wood'
            when ht.ha_task_name like '%Vinyl%' and ht.ha_task_name not like '%Repair%' and ht.ha_task_name not like '%Business%' then 'Vinyl'
            else 'Other'
        end as ha_type,
        case
            when ht.ha_task_name like "%Business%" then "Commercial"
            when pm.id = 2 and ps.label like "%Install a Wood Fence%" then 'Wood Fence-CN-EB'
            when pm.id = 10 and ps.label like "%Install a Wood Fence%" then 'Wood Fence-CN-FR'
            when pm.id = 9 and ps.label like "%Install a Wood Fence%" then 'Wood Fence-CN-NB'
            when pm.id = 3 and ps.label like "%Install a Wood Fence%" then 'Wood Fence-CN-SA'
            when pm.id = 29 and ps.label like "%Install a Wood Fence%" then 'Wood Fence-CN-ST'
            when pm.id = 4 and ps.label like "%Install a Wood Fence%" then 'Wood Fence-CN-WA'
            when pm.id = 31 and ps.label like "%Install a Wood Fence%" then 'Wood Fence-CN-PA'
            when pm.id = 30 and ps.label like "%Install a Wood Fence%" then 'Wood Fence-CN-SJ'
            when pm.id = 8 and ps.label like "%Install a Wood Fence%" then 'Wood Fence-CN-SF'
            --when pm.id = 13 and ps.label like "%Install a Wood Fence%" then 'Wood Fence-CN-Rest'
            when pm.id = 6 and ps.label like "%Install a Wood Fence%" then 'Wood Fence-CS-LA'
            when pm.id = 5 and ps.label like "%Install a Wood Fence%" then 'Wood Fence-CS-OC'
            when pm.id = 14 and ps.label like "%Install a Wood Fence%" then 'Wood Fence-CS-SV'
            when pm.id = 7 and ps.label like "%Install a Wood Fence%" then 'Wood Fence-CS-VC'
            when pm.id = 1 and ps.label like "%Install a Wood Fence%" then 'Wood Fence-CS-SD'
            --when pm.id in (12,11) and ps.label like "%Install a Wood Fence%" then 'Wood Fence-CS-Rest'
            when pm.id = 16 and ps.label like "%Install a Wood Fence%" then 'Wood Fence-TX-DL'
            when pm.id = 17 and ps.label like "%Install a Wood Fence%" then 'Wood Fence-TX-FW'
            when pm.id = 18 and ps.label like "%Install a Wood Fence%" then 'Wood Fence-TX-HT'
            when pm.id = 19 and ps.label like "%Install a Wood Fence%" then 'Wood Fence-TX-SA'
            when pm.id = 32 and ps.label like "%Install a Wood Fence%" then 'Wood Fence-TX-AU'
            when pm.id = 20 and ps.label like "%Install a Wood Fence%" then 'Wood Fence-GA-AT'
            when pm.id = 22 and ps.label like "%Install a Wood Fence%" then 'Wood Fence-MD-BL'
            when pm.id = 21 and ps.label like "%Install a Wood Fence%" then 'Wood Fence-MD-DC'
            when pm.id = 33 and ps.label like "%Install a Wood Fence%" then 'Wood Fence-PA-PH'
            when pm.id = 35 and ps.label like "%Install a Wood Fence%" then 'Wood Fence-VA-AR'
            when pm.id = 24 and ps.label like "%Install a Wood Fence%" then 'Wood Fence-FL-MI'
            when pm.id = 43 and ps.label like "%Install a Wood Fence%" then 'Wood Fence-WA-SE'
            when pm.id in (2, 10, 9, 3, 29, 4, 31, 30, 8, 13) and ps.label like "%Repair%" then 'Repairs Fence-CN'
            when pm.id in (6, 5, 14, 7, 1, 12, 11) and ps.label like "%Repair%" then 'Repairs Fence-CS'
            when pm.code like '%-TX-%' and ps.label like "%Repair%" then 'Repairs Fence-TX'
            when pm.code like '%-GA-%' and ps.label like "%Repair%" then 'Repairs Fence-GA'
            when pm.id = 2 and ps.label like "%Vinyl%" then 'Vinyl Fence-CN-EB'
            when pm.id = 10 and ps.label like "%Vinyl%" then 'Vinyl Fence-CN-FR'
            when pm.id = 9 and ps.label like "%Vinyl%" then 'Vinyl Fence-CN-NB'
            when pm.id = 3 and ps.label like "%Vinyl%" then 'Vinyl Fence-CN-SA'
            when pm.id = 29 and ps.label like "%Vinyl%" then 'Vinyl Fence-CN-ST'
            when pm.id = 4 and ps.label like "%Vinyl%" then 'Vinyl Fence-CN-WA'
            when pm.id = 31 and ps.label like "%Vinyl%" then 'Vinyl Fence-CN-PA'
            when pm.id = 30 and ps.label like "%Vinyl%" then 'Vinyl Fence-CN-SJ'
            when pm.id = 8 and ps.label like "%Vinyl%" then 'Vinyl Fence-CN-SF'
            when pm.id = 6 and ps.label like "%Vinyl%" then 'Vinyl Fence-CS-LA'
            when pm.id = 5 and ps.label like "%Vinyl%" then 'Vinyl Fence-CS-OC'
            when pm.id = 14 and ps.label like "%Vinyl%" then 'Vinyl Fence-CS-SV'
            when pm.id = 7 and ps.label like "%Vinyl%" then 'Vinyl Fence-CS-VC'
            when pm.id = 1 and ps.label like "%Vinyl%" then 'Vinyl Fence-CS-SD'
            --when pm.id in (12,11) and ps.label like "%Vinyl%"  then 'Vinyl Fence-CS-Rest'
            when pm.id = 22 and ps.label like "%Vinyl%" then 'Vinyl Fence-MD-BL'
            when pm.id = 21 and ps.label like "%Vinyl%" then 'Vinyl Fence-MD-DC'
            when pm.id = 33 and ps.label like "%Vinyl%" then 'Vinyl Fence-PA-PH'
            when pm.id = 35 and ps.label like "%Vinyl%" then 'Vinyl Fence-VA-AR'
            when pm.id = 24 and ps.label like "%Vinyl%" then 'Vinyl Fence-FL-MI'
            when pm.id in (2, 10, 9, 3, 29, 4, 31, 30, 8, 13) and ps.label like "%Chain%" then 'CL Fence-CN'
            when pm.id in (6, 5, 14, 7, 1, 12, 11) and ps.label like "%Chain%" then 'CL Fence-CS'
            when pm.code like '%-TX-%' and ps.label like "%Chain%" then 'CL Fence-TX'
            when pm.code like '%-GA-%' and ps.label like "%Chain%" then 'CL Fence-GA'
            when pm.id in (2, 10, 9, 3, 29, 4, 31, 30, 8, 13) and ps.product_id = 34 then 'Hardscape-CN'
            when pm.id in (6, 5, 14, 7, 1, 12, 11) and ps.product_id = 34 then 'Hardscape-CS'
            when pm.code like '%-TX-%' and ps.product_id = 34 then 'Hardscape-TX'
            when pm.id = 30 and ps.label = 'Install Artificial Grass' then 'Artificial Grass-CN-SJ'
            when pm.id = 31 and ps.label = 'Install Artificial Grass' then 'Artificial Grass-CN-PA'
            when pm.id = 2 and ps.label = 'Install Artificial Grass' then 'Artificial Grass-CN-EB'
            when pm.id = 3 and ps.label = 'Install Artificial Grass' then 'Artificial Grass-CN-SA'
            when pm.id = 29 and ps.label = 'Install Artificial Grass' then 'Artificial Grass-CN-ST'
            when pm.id = 9 and ps.label = 'Install Artificial Grass' then 'Artificial Grass-CN-NB'
            when pm.id = 8 and ps.label = 'Install Artificial Grass' then 'Artificial Grass-CN-SF'
            when pm.id = 4 and ps.label = 'Install Artificial Grass' then 'Artificial Grass-CN-WA'
            when pm.id = 10 and ps.label = 'Install Artificial Grass' then 'Artificial Grass-CN-FR'
            when pm.id = 6 and ps.label = 'Install Artificial Grass' then 'Artificial Grass-CS-LA'
            when pm.id = 5 and ps.label = 'Install Artificial Grass' then 'Artificial Grass-CS-OC'
            when pm.id = 14 and ps.label = 'Install Artificial Grass' then 'Artificial Grass-CS-SV'
            when pm.id = 7 and ps.label = 'Install Artificial Grass' then 'Artificial Grass-CS-VC'
            when ps.label like '%Staining%' then 'Staining-US'
            else 'Other'
        end as segment,
        case
            when pm.id in (8, 30, 31, 4) and pm.code like '%-%' then 'PA-CA-SB' --ids for 'CN-SF', 'CN-SJ', 'CN-PA', 'CN-WA'
            when pm.id in (3, 29) and pm.code like '%-%' then 'PA-CA-SA' --ids for 'CN-SA', 'CN-ST'
            when pm.code like '%-%-%' then pm.code
            else 'Unknown'
        end as finance_market,
        cast(mv.volume as bignumeric) as volume,
        row_number() over (order by date, hm.ha_msa_name) as row_num
    from
        int_data.ha_msa_volume mv left join
        int_data.ha_msa hm on hm.ha_msa_id = mv.ha_msa_id left join
        int_data.ha_task ht on ht.ha_task_id = mv.ha_task_id left join
        int_data.ha_msa_map hmm on hmm.ha_msa_id = mv.ha_msa_id left join
        ergeon.product_market pm on pm.id = hmm.ergeon_market_id left join
        ergeon.geo_msa em on em.id = hmm.ergeon_msa_id left join
        ergeon.product_service ps on ps.id = ht.ergeon_task_id left join
        ergeon.store_product sp on sp.id = ps.product_id left join
        ergeon.product_region pr on pr.id = pm.region_id
)

select
    m.*,
    format_datetime("%Y %b", date) || " - " || ha_task_name as year_month_ha_task,
    gs.id as ergeon_state_id,
    case
        when finance_market is not null and ha_type = 'Wood' then finance_market
        when finance_market is not null and ha_type = 'Chain Link' then finance_market
        when finance_market in ('PA-CA-SD', 'PA-CA-VC', 'PA-CA-LA', 'PA-CA-OC', 'PA-CA-SV') and ha_type = 'Vinyl' then finance_market
        when finance_market is null then 'Unknown'
        else 'Other'
    end as finance_segment,
    row_number() over (order by date, ha_task_name, ergeon_region_name, finance_market, ha_msa_name) as row_num_task

from
    msa_data m left join
    ergeon.geo_state gs on gs.code = m.ha_state_name
order by row_num_task
