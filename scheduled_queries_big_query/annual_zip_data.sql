with unique_fence_zip as (
    select distinct
        z.code as zipcode
    from
        ergeon.geo_zipcode z join
        ergeon.product_tierzipcode tz on tz.zip_code_id = z.id join
        ergeon.product_tier t on t.id = tz.tier_id join
        ergeon.product_tierproduct tp on tp.tier_id = t.id
    where tp.product_id = 105
        and t.status_id <> 64
),

annual_zip_data as (
    select
        row_number() over (order by hz.date, ht.ha_task_id) as row_num,
        hz.date + 1 as date,
        hz.ergeon_zipcode,
        ht.ha_task_id,
        ht.ha_task_name,
        case
            when gc.name is null then 'Unknown'
            else gc.name
        end as county_name,
        ps.label as ergeon_task_name,
        case
            when sp.name is null then 'Unknown'
            else sp.name
        end as ergeon_product,
        pm.name as ergeon_market_name,
        case
            when pm.id in (2, 10, 9, 3, 29, 4, 31, 30, 8, 13) then 'North California'
            when pm.id in (6, 5, 14, 7, 1, 12, 11) then 'South California'
            when pr.name = 'West South Central' then 'Texas'
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
        hm.ha_msa_name,
        case
            when em.name is null then 'Unknown'
            else em.name
        end as ergeon_msa_name,
        case
            when gs.code is null then 'Unknown'
            else gs.code
        end as state_name,
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
            when pm.id = 6 and ps.label like "%Install a Wood Fence%" then 'Wood Fence-CS-LA'
            when pm.id = 5 and ps.label like "%Install a Wood Fence%" then 'Wood Fence-CS-OC'
            when pm.id = 14 and ps.label like "%Install a Wood Fence%" then 'Wood Fence-CS-SV'
            when pm.id = 7 and ps.label like "%Install a Wood Fence%" then 'Wood Fence-CS-VC'
            when pm.id = 1 and ps.label like "%Install a Wood Fence%" then 'Wood Fence-CS-SD'
            when pm.id = 16 and ps.label like "%Install a Wood Fence%" then 'Wood Fence-TX-DL'
            when pm.id = 17 and ps.label like "%Install a Wood Fence%" then 'Wood Fence-TX-FW'
            when pm.id = 18 and ps.label like "%Install a Wood Fence%" then 'Wood Fence-TX-HT'
            when pm.id = 19 and ps.label like "%Install a Wood Fence%" then 'Wood Fence-TX-SA'
            when pm.id = 32 and ps.label like "%Install a Wood Fence%" then 'Wood Fence-TX-AU'
            when pm.id = 20 and ps.label like "%Install a Wood Fence%" then 'Wood Fence-GA-AT'
            when pm.id = 22 and ps.label like "%Install a Wood Fence%" then 'Wood Fence-MD-BL'
            when pm.id = 21 and ps.label like "%Install a Wood Fence%" then 'Wood Fence-MD-DC'
            when pm.id = 35 and ps.label like "%Install a Wood Fence%" then 'Wood Fence-VA-AR'
            when pm.id = 33 and ps.label like "%Install a Wood Fence%" then 'Wood Fence-PA-PH'
            when pm.id = 24 and ps.label like "%Install a Wood Fence%" then 'Wood Fence-FL-MI'
            when pm.id = 26 and ps.label like "%Install a Wood Fence%" then 'Wood Fence-FL-OR'
            when pm.id in (2, 10, 9, 3, 29, 4, 31, 30, 8, 13) and ps.label like "%Repair%" then 'Repairs Fence-CN'
            when pm.id in (6, 5, 14, 7, 1, 12, 11) and ps.label like "%Repair%" then 'Repairs Fence-CS'
            when pr.name = 'West South Central' and ps.label like "%Repair%" then 'Repairs Fence-TX'
            when pr.name = 'South Atlantic' and ps.label like "%Repair%" then 'Repairs Fence-SA'
            when pm.code like '%-MD-%' and ps.label like "%Repair%" then 'Repairs Fence-MD'
            when pm.code like '%-VA-%' and ps.label like "%Repair%" then 'Repairs Fence-VA'
            when pm.code like '%-PA-%' and ps.label like "%Repair%" then 'Repairs Fence-PA'
            when pm.code like '%-FL-%' and ps.label like "%Repair%" then 'Repairs Fence-FL'
            when pm.id = 6 and ps.label like "%Vinyl%" then 'Vinyl Fence-CS-LA'
            when pm.id = 5 and ps.label like "%Vinyl%" then 'Vinyl Fence-CS-OC'
            when pm.id = 14 and ps.label like "%Vinyl%" then 'Vinyl Fence-CS-SV'
            when pm.id = 7 and ps.label like "%Vinyl%" then 'Vinyl Fence-CS-VC'
            when pm.id = 1 and ps.label like "%Vinyl%" then 'Vinyl Fence-CS-SD'
            when pm.id = 22 and ps.label like "%Vinyl%" then 'Vinyl Fence-MD-BL'
            when pm.id = 21 and ps.label like "%Vinyl%" then 'Vinyl Fence-MD-DC'
            when pm.id = 33 and ps.label like "%Vinyl%" then 'Vinyl Fence-VA-AR'
            when pm.id = 35 and ps.label like "%Vinyl%" then 'Vinyl Fence-PA-PH'
            when pm.id = 24 and ps.label like "%Vinyl%" then 'Vinyl Fence-FL-MI'
            when pm.id = 26 and ps.label like "%Vinyl%" then 'Vinyl Fence-FL-OR'
            when pm.id = 2 and ps.label like "%Install Artificial Grass%" then 'Artificial Grass-CN-EB'
            when pm.id = 31 and ps.label like "%Install Artificial Grass%" then 'Artificial Grass-CN-PA'
            when pm.id = 30 and ps.label like "%Install Artificial Grass%" then 'Artificial Grass-CN-SJ'
            when pm.id = 29 and ps.label like "%Install Artificial Grass%" then 'Artificial Grass-CN-SA'
            when pm.id = 9 and ps.label like "%Install Artificial Grass%" then 'Artificial Grass-CN-NB'
            when pm.id = 8 and ps.label like "%Install Artificial Grass%" then 'Artificial Grass-CN-SF'
            when pm.id = 4 and ps.label like "%Install Artificial Grass%" then 'Artificial Grass-CN-WA'
            when pm.id = 10 and ps.label like "%Install Artificial Grass%" then 'Artificial Grass-CN-FR'
            when pm.id in (2, 10, 9, 3, 29, 4, 31, 30, 8, 13) and ps.label like "%Chain%" then 'CL Fence-CN'
            when pm.id in (6, 5, 14, 7, 1, 12, 11) and ps.label like "%Chain%" then 'CL Fence-CS'
            when pr.name = 'West South Central' and ps.label like "%Chain%" then 'CL Fence-TX'
            when pr.name = 'South Atlantic' and ps.label like "%Chain%" then 'CL Fence-SA'
            when pm.id in (2, 10, 9, 3, 29, 4, 31, 30, 8, 13) and ps.label like "/Driveway%" then 'Hardscape-CN'
            when pm.id in (6, 5, 14, 7, 1, 12, 11) and ps.label like "/Driveway%" then 'Hardscape-CS'
            when pr.name = 'West South Central' and ps.label like "/Driveway%" then 'Hardscape-TX'
            when ps.label like '%Staining%' then 'Staining-US'
            else 'Other'
        end as segment,
        case when ufz.zipcode is null then 'No' else 'Yes' end as ergeon_tier,
        case
            when pm.id in (8, 30, 31, 4) and pm.code like '%-%' then 'PA-CA-SB' --ids for 'CN-SF', 'CN-SJ', 'CN-PA', 'CN-WA'
            when pm.id in (3, 29) and pm.code like '%-%' then 'PA-CA-SA' --ids for 'CN-SA', 'CN-ST'
            when pm.code like '%-%-%' then pm.code
            else 'Other'
        end as finance_market,
        hz.volume
    from
        int_data.ha_zipcode_volume hz left join
        int_data.ha_task ht on ht.ha_task_id = hz.ha_task_id left join
        int_data.ha_msazipcode hmz on hmz.ergeon_zipcode = hz.ergeon_zipcode left join
        int_data.ha_msa hm on hm.ha_msa_id = hmz.ha_msa_id left join
        ergeon.geo_zipcode gz on gz.code = hz.ergeon_zipcode left join
        ergeon.geo_county gc on gc.id = gz.county_id left join
        ergeon.product_countymarket pcm on pcm.county_id = gc.id left join
        ergeon.product_market pm on pm.id = pcm.market_id left join
        ergeon.product_region pr on pr.id = pm.region_id left join
        ergeon.geo_msa em on em.id = gc.msa_id left join
        ergeon.geo_state gs on gs.id = gc.state_id left join
        ergeon.product_service ps on ps.id = ht.ergeon_task_id left join
        ergeon.store_product sp on sp.id = ps.product_id left join
        unique_fence_zip ufz on ufz.zipcode = hz.ergeon_zipcode
),

finance_market_two as (
    select
        *,
        case
            when
                (
                    state_name = 'CA' and ergeon_tier = 'Yes'
                ) or (finance_market in ('WS-TX-DL', 'WS-TX-FW', 'WS-TX-HT') and ergeon_tier = 'Yes') then finance_market
            when
                (
                    state_name = 'CA' and ergeon_tier = 'No'
                ) or (finance_market in ('WS-TX-DL', 'WS-TX-FW', 'WS-TX-HT') and ergeon_tier = 'No') then 'Other'
            when state_name <> 'CA' and finance_market not in ('WS-TX-DL', 'WS-TX-FW', 'WS-TX-HT') then finance_market
            when finance_market = 'Other' then 'Other'
            else 'Unknown'
        end as finance_market_2
    from
        annual_zip_data
)

select
    *,
    case
        when ha_task_id in (40052, 40058, 40059) then finance_market_2
        when ha_task_id = 40254 and finance_market_2 like '%-CS-%' then finance_market_2
        when ha_task_id = 40254 and finance_market_2 like '%-FL-%' then finance_market_2
        when ha_task_id = 40254 and finance_market_2 like '%-PA-%' then finance_market_2
        when ha_task_id = 40254 and finance_market_2 like '%-MD-%' then finance_market_2
        else finance_market_2
    end as finance_segment
from finance_market_two
