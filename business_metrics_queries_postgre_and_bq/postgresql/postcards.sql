-- upload to BQ
select
 date_trunc('{period}', pe.created_at at time zone 'America/Los_Angeles')::date as date,
 count(*) as MAR303,
 sum(case when p.size = '6x9' then 0.46
                   when p.size = '6x11' then 0.51 end) as MAR304,
 sum(case when p.size = '6x9' and p.product_id = 105 then 0.46
                         when p.size = '6x11' and p.product_id = 105 then 0.51 else 0 end) as MAR304F,
 sum(case when p.size = '6x9' and p.product_id = 34 then 0.46
                         when p.size = '6x11' and p.product_id = 34 then 0.51 else 0 end) as MAR304D,
 sum(case when p.product_id = 105 then 1 else 0 end) MAR305,
 sum(case when p.product_id = 34 then 1 else 0 end) as MAR306,
 sum(case when campaign_name = 'on_lead_arrived' then 1 else 0 end) as MAR309,
 sum(case when campaign_name = 'driveway_blueprints' then 1 else 0 end) as MAR310,
 sum(case when campaign_name = 'on_address_arrived' then 1 else 0 end) as MAR308,
 sum(case when campaign_name = 'on_completion_neighbors' and pe.created_at > '2021-03-16'then 1 else 0 end) as MAR311,
 sum(case when p.product_id = 132 then 1 else 0 end) as MAR2321
from marketing_notifications_directmailevent pe
left join marketing_notifications_directmail dm on dm.id = pe.direct_mail_id 
left join marketing_notifications_directmailtemplate p on p.id = dm.direct_mail_template_id 
where pe.name = 'scheduled'
and pe.created_at at time zone 'America/Los_Angeles' >= '2018-04-16'
group by 1
order by 1
