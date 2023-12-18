CREATE TEMP FUNCTION is_trailing_period(input_date DATE, n INT64)
RETURNS STRING
AS (
  (if(date_diff(current_date('America/Los_Angeles'),input_date,day) = 0,'Today',
  if(date_diff(current_date('America/Los_Angeles'),input_date,day) between 1 and n,'Period-A',
  if(date_diff(current_date('America/Los_Angeles'),input_date,day) between n+1 and 2*n,'Period-B',
  if(date_diff(current_date('America/Los_Angeles'),input_date,day) between 2*n+1 and 3*n,'Period-C',
  if(date_diff(current_date('America/Los_Angeles'),input_date,day) between 3*n+1 and 4*n,'Period-D',
  if(date_diff(current_date('America/Los_Angeles'),input_date,day) between 4*n+1 and 5*n,'Period-E',
  if(date_diff(current_date('America/Los_Angeles'),input_date,day) between 5*n+1 and 6*n,'Period-F',
  if(date_diff(current_date('America/Los_Angeles'),input_date,day) between 6*n+1 and 7*n,'Period-G',
  if(date_diff(current_date('America/Los_Angeles'),input_date,day) between 7*n+1 and 8*n,'Period-H',
  if(date_diff(current_date('America/Los_Angeles'),input_date,day) between 8*n+1 and 9*n,'Period-I',
  if(date_diff(current_date('America/Los_Angeles'),input_date,day) between 9*n+1 and 10*n,'Period-J',
  if(input_date is null,'Period-Null','Period-Rest')))))))))))))
);

with extra_dimensions as (
  select 
    dim.order_id,
    case --The reason of using "if(parent_order_id is null,dim.channel,parent_order_dim.channel)" is because the channel of wwo's is always Unknown since they don't have lead_id. As a result, when order is wwo we are capturing the channel of it's parent order id
      when (if(parent_order_id is null,dim.channel,parent_order_dim.channel) like '%Home Advisor%' -- if channel is Home Advisor
              and coalesce(if(parent_order_id is null,dim.ha_type,parent_order_dim.ha_type),if(parent_order_id is null,dim.lead_channel,parent_order_dim.lead_channel)) = '/C' --and (if HA lead is business or lead channel type is commercial) then Commercial Segment
            ) 
        or (if(parent_order_id is null,dim.channel,parent_order_dim.channel) not like '%Home Advisor%' --if channel is not Home Advisor
              and (if(parent_order_id is null,dim.lead_channel,parent_order_dim.lead_channel) = '/C' or if(parent_order_id is null,dim.type,parent_order_dim.type) = "/Commercial")) --and (if HA lead is business or customer type is commercial) then Commercial Segment 
      then 'Commercial'
    -- residential
      when dim.market_id = 2 and dim.product_quoted like "%Install a Wood Fence%" then 'Wood Fence-CN-EB'
      when dim.market_id = 10 and dim.product_quoted like "%Install a Wood Fence%" then 'Wood Fence-CN-FR'
      when dim.market_id = 9 and dim.product_quoted like "%Install a Wood Fence%" then 'Wood Fence-CN-NB'
      when dim.market_id = 3 and dim.product_quoted like "%Install a Wood Fence%" then 'Wood Fence-CN-SA'
      when dim.market_id = 29 and dim.product_quoted like "%Install a Wood Fence%" then 'Wood Fence-CN-ST'
      when dim.market_id = 4 and dim.product_quoted like "%Install a Wood Fence%" then 'Wood Fence-CN-WA'
      when dim.market_id = 31 and dim.product_quoted like "%Install a Wood Fence%" then 'Wood Fence-CN-PA'
      when dim.market_id = 30 and dim.product_quoted like "%Install a Wood Fence%" then 'Wood Fence-CN-SJ'
      when dim.market_id = 8 and dim.product_quoted like "%Install a Wood Fence%" then 'Wood Fence-CN-SF'
      --when dim.market_id = 13 and product_quoted like "%Install a Wood Fence%" then 'Wood Fence-CN-Rest'
      when dim.market_id = 6 and dim.product_quoted like "%Install a Wood Fence%" then 'Wood Fence-CS-LA'
      when dim.market_id = 5 and dim.product_quoted like "%Install a Wood Fence%" then 'Wood Fence-CS-OC'
      when dim.market_id = 14 and dim.product_quoted like "%Install a Wood Fence%" then 'Wood Fence-CS-SV'
      when dim.market_id = 7 and dim.product_quoted like "%Install a Wood Fence%" then 'Wood Fence-CS-VC'
      when dim.market_id = 1 and dim.product_quoted like "%Install a Wood Fence%" then 'Wood Fence-CS-SD'
      --when dim.market_id in (12,11) and product_quoted like "%Install a Wood Fence%" then 'Wood Fence-CS-Rest'
      when dim.market_id = 16 and dim.product_quoted like "%Install a Wood Fence%" then 'Wood Fence-TX-DL'
      when dim.market_id = 17 and dim.product_quoted like "%Install a Wood Fence%" then 'Wood Fence-TX-FW'
      when dim.market_id = 18 and dim.product_quoted like "%Install a Wood Fence%" then 'Wood Fence-TX-HT'
      when dim.market_id = 19 and dim.product_quoted like "%Install a Wood Fence%" then 'Wood Fence-TX-SA'
      when dim.market_id = 32 and dim.product_quoted like "%Install a Wood Fence%" then 'Wood Fence-TX-AU'
      when dim.market_id = 20 and dim.product_quoted like "%Install a Wood Fence%" then 'Wood Fence-GA-AT'
      when dim.market_id = 22 and dim.product_quoted like "%Install a Wood Fence%" then 'Wood Fence-MD-BL'
      when dim.market_id = 21 and dim.product_quoted like "%Install a Wood Fence%" then 'Wood Fence-MD-DC'
      when dim.market_id = 33 and dim.product_quoted like "%Install a Wood Fence%" then 'Wood Fence-PA-PH'
      when dim.market_id = 35 and dim.product_quoted like "%Install a Wood Fence%" then 'Wood Fence-VA-AR'
      when dim.market_id = 24 and dim.product_quoted like "%Install a Wood Fence%" then 'Wood Fence-FL-MI'
      when dim.market_id = 43 and dim.product_quoted like "%Install a Wood Fence%" then 'Wood Fence-WA-SE'
      when dim.market_id in (2,10,9,3,29,4,31,30,8,13) and dim.product_quoted like "%Repair%" then 'Repairs Fence-CN'
      when dim.market_id in (6,5,14,7,1,12,11) and dim.product_quoted like "%Repair%" then 'Repairs Fence-CS'
      when dim.market like '%-TX-%' and dim.product_quoted like "%Repair%" then 'Repairs Fence-TX'
      when dim.market like '%-GA-%' and dim.product_quoted like "%Repair%" then 'Repairs Fence-GA'
      when dim.market_id = 2 and dim.product_quoted like "%Vinyl%" then 'Vinyl Fence-CN-EB'
      when dim.market_id = 10 and dim.product_quoted like "%Vinyl%" then 'Vinyl Fence-CN-FR'
      when dim.market_id = 9 and dim.product_quoted like "%Vinyl%" then 'Vinyl Fence-CN-NB'
      when dim.market_id = 3 and dim.product_quoted like "%Vinyl%" then 'Vinyl Fence-CN-SA'
      when dim.market_id = 29 and dim.product_quoted like "%Vinyl%" then 'Vinyl Fence-CN-ST'
      when dim.market_id = 4 and dim.product_quoted like "%Vinyl%" then 'Vinyl Fence-CN-WA'
      when dim.market_id = 31 and dim.product_quoted like "%Vinyl%" then 'Vinyl Fence-CN-PA'
      when dim.market_id = 30 and dim.product_quoted like "%Vinyl%" then 'Vinyl Fence-CN-SJ'
      when dim.market_id = 8 and dim.product_quoted like "%Vinyl%" then 'Vinyl Fence-CN-SF'
      when dim.market_id = 6 and dim.product_quoted like "%Vinyl%"  then 'Vinyl Fence-CS-LA'
      when dim.market_id = 5 and dim.product_quoted like "%Vinyl%"  then 'Vinyl Fence-CS-OC'
      when dim.market_id = 14 and dim.product_quoted like "%Vinyl%"  then 'Vinyl Fence-CS-SV'
      when dim.market_id = 7 and dim.product_quoted like "%Vinyl%"  then 'Vinyl Fence-CS-VC'
      when dim.market_id = 1 and dim.product_quoted like "%Vinyl%"  then 'Vinyl Fence-CS-SD'
      --when dim.market_id in (12,11) and product_quoted like "%Vinyl%"  then 'Vinyl Fence-CS-Rest'
      when dim.market_id = 22 and dim.product_quoted like "%Vinyl%"  then 'Vinyl Fence-MD-BL'
      when dim.market_id = 21 and dim.product_quoted like "%Vinyl%"  then 'Vinyl Fence-MD-DC'
      when dim.market_id = 33 and dim.product_quoted like "%Vinyl%"  then 'Vinyl Fence-PA-PH'
      when dim.market_id = 35 and dim.product_quoted like "%Vinyl%"  then 'Vinyl Fence-VA-AR'
      when dim.market_id = 24 and dim.product_quoted like "%Vinyl%"  then 'Vinyl Fence-FL-MI'
      when dim.market_id in (2,10,9,3,29,4,31,30,8,13) and dim.product_quoted like "%Chain%"  then 'CL Fence-CN'
      when dim.market_id in (6,5,14,7,1,12,11) and dim.product_quoted like "%Chain%"  then 'CL Fence-CS'
      when dim.market like '%-TX-%' and dim.product_quoted like "%Chain%"  then 'CL Fence-TX'
      when dim.market like '%-GA-%' and dim.product_quoted like "%Chain%"  then 'CL Fence-GA'
      when dim.market_id in (2,10,9,3,29,4,31,30,8,13) and dim.product_quoted like "/Driveway%"  then 'Hardscape-CN'
      when dim.market_id in (6,5,14,7,1,12,11) and dim.product_quoted like "/Driveway%"  then 'Hardscape-CS' 
      when dim.market like '%-TX-%' and dim.product_quoted like "/Driveway%"  then 'Hardscape-TX' 
      when dim.market_id = 30 and dim.product_quoted = '/Landscaping/Install Artificial Grass' then 'Artificial Grass-CN-SJ'
      when dim.market_id = 31 and dim.product_quoted = '/Landscaping/Install Artificial Grass' then 'Artificial Grass-CN-PA'
      when dim.market_id = 2 and dim.product_quoted = '/Landscaping/Install Artificial Grass' then 'Artificial Grass-CN-EB'
      when dim.market_id = 3 and dim.product_quoted = '/Landscaping/Install Artificial Grass' then 'Artificial Grass-CN-SA'
      when dim.market_id = 29 and dim.product_quoted = '/Landscaping/Install Artificial Grass' then 'Artificial Grass-CN-ST'
      when dim.market_id = 9 and dim.product_quoted = '/Landscaping/Install Artificial Grass' then 'Artificial Grass-CN-NB'
      when dim.market_id = 8 and dim.product_quoted = '/Landscaping/Install Artificial Grass' then 'Artificial Grass-CN-SF'
      when dim.market_id = 4 and dim.product_quoted = '/Landscaping/Install Artificial Grass' then 'Artificial Grass-CN-WA'
      when dim.market_id = 10 and dim.product_quoted = '/Landscaping/Install Artificial Grass' then 'Artificial Grass-CN-FR'
      when dim.market_id = 6 and dim.product_quoted = '/Landscaping/Install Artificial Grass' then 'Artificial Grass-CS-LA'
      when dim.market_id = 5 and dim.product_quoted = '/Landscaping/Install Artificial Grass' then 'Artificial Grass-CS-OC'
      when dim.market_id = 14 and dim.product_quoted = '/Landscaping/Install Artificial Grass' then 'Artificial Grass-CS-SV'
      when dim.market_id = 7 and dim.product_quoted = '/Landscaping/Install Artificial Grass' then 'Artificial Grass-CS-VC'
      when dim.product_quoted like '%Staining%' then 'Staining-US'
      else 'Other'
    end as segment,
    case --The reason of using "if(parent_order_id is null,dim.channel,parent_order_dim.channel)" is because the channel of wwo's is always Unknown since they don't have lead_id. As a result, when order is wwo we are capturing the channel of it's parent order id
      when (if(parent_order_id is null,dim.channel,parent_order_dim.channel) like '%Home Advisor%' -- if channel is Home Advisor
              and coalesce(if(parent_order_id is null,dim.ha_type,parent_order_dim.ha_type),if(parent_order_id is null,dim.lead_channel,parent_order_dim.lead_channel)) = '/C' --and (if HA lead is business or lead channel is commercial) then Commercial Segment
            ) 
        or (if(parent_order_id is null,dim.channel,parent_order_dim.channel) not like '%Home Advisor%' --if channel is not Home Advisor
              and (if(parent_order_id is null,dim.lead_channel,parent_order_dim.lead_channel) = '/C' or if(parent_order_id is null,dim.type,parent_order_dim.type) = "/Commercial")) --and (if lead type is commercial or customer type is commercial) then Commercial Segment 
      then 'Commercial'
      when dim.market_id in (2,10,9,3,29,4,31,30,8,13) and dim.product_quoted like "%Install a Wood Fence%" then 'Wood Fence-CN' 
	    when dim.market_id in (6,5,14,7,1,12,11) and dim.product_quoted like "%Install a Wood Fence%" then 'Wood Fence-CS'
      when dim.market like '%-TX-%' and dim.product_quoted like "%Install a Wood Fence%" then 'Wood Fence-TX'
      when dim.market like '%-GA-%' and dim.product_quoted like "%Install a Wood Fence%" then 'Wood Fence-GA'
      when dim.market like '%-MD-%' and dim.product_quoted like "%Install a Wood Fence%" then 'Wood Fence-MD'
      when dim.market like '%-PA-%' and dim.product_quoted like "%Install a Wood Fence%" then 'Wood Fence-PA'
      when dim.market like '%-VA-%' and dim.product_quoted like "%Install a Wood Fence%" then 'Wood Fence-VA'
      when dim.market like '%-FL-%' and dim.product_quoted like "%Install a Wood Fence%" then 'Wood Fence-FL'
      when dim.market like '%-WA-%' and dim.product_quoted like "%Install a Wood Fence%" then 'Wood Fence-WA' 
      when dim.market_id in (2,10,9,3,29,4,31,30,8,13) and dim.product_quoted like "%Repair%" then 'Repairs Fence-CN'
      when dim.market_id in (6,5,14,7,1,12,11) and dim.product_quoted like "%Repair%" then 'Repairs Fence-CS'
      when dim.market like '%-TX-%' and dim.product_quoted like "%Repair%" then 'Repairs Fence-TX'
      when dim.market like '%GA-%' and dim.product_quoted like "%Repair%" then 'Repairs Fence-GA'
      when dim.market_id in (2,10,9,3,29,4,31,30,8,13) and dim.product_quoted like "%Vinyl%" then 'Vinyl Fence-CN' 
      when dim.market_id in (6,5,14,7,1,12,11) and dim.product_quoted like "%Vinyl%" then 'Vinyl Fence-CS'
      when dim.market like '%-MD-%' and dim.product_quoted like "%Vinyl%" then 'Vinyl Fence-TX'
      when dim.market like '%-PA-%' and dim.product_quoted like "%Vinyl%" then 'Vinyl Fence-PA'
      when dim.market like '%-VA-%' and dim.product_quoted like "%Vinyl%" then 'Vinyl Fence-VA'
      when dim.market like '%-FL-%' and dim.product_quoted like "%Vinyl%" then 'Vinyl Fence-FL'
      when dim.market_id in (2,10,9,3,29,4,31,30,8,13) and dim.product_quoted like "%Chain%" then 'CL Fence-CN' 
	    when dim.market_id in (6,5,14,7,1,12,11) and dim.product_quoted like "%Chain%" then 'CL Fence-CS'
      when dim.market like '%-TX-%' and dim.product_quoted like "%Chain%" then 'CL Fence-TX'
      when dim.market like '%-GA-%' and dim.product_quoted like "%Chain%" then 'CL Fence-GA'
      when dim.market_id in (2,10,9,3,29,4,31,30,8,13) and dim.product_quoted like "/Driveway%"  then 'Hardscape-CN'
      when dim.market_id in (6,5,14,7,1,12,11) and dim.product_quoted like "/Driveway%"  then 'Hardscape-CS' 
      when dim.market like '%-TX-%' and dim.product_quoted like "/Driveway%"  then 'Hardscape-TX' 
      when dim.product_quoted = '/Landscaping/Install Artificial Grass' and dim.market_id in (2,10,9,3,29,4,31,30,8,13) then 'Artificial Grass-CN'
      when dim.product_quoted = '/Landscaping/Install Artificial Grass' and dim.market_id in (6,5,14,7,1,12,11) then 'Artificial Grass-CS'
      when dim.product_quoted like '%Staining%' then 'Staining-US'
      else 'Other' 
    end as segment_l1,
    case 
      when dim.market_id in (2,10,9,3,29,4,31,30,8,13) then 'North California'
      when dim.market_id in (6,5,14,7,1,12,11) then 'South California'
      when dim.market like '%-TX-%' then 'Texas'
      when dim.market like '%-GA-%' then 'Georgia'
      when dim.market like '%-MD-%' then 'Maryland'
      when dim.market like '%-PA-%' then 'Pennsylvania'
      when dim.market like '%-VA-%' then 'Virginia'
      when dim.market like '%-FL-%' then 'Florida'
      When dim.market like '%-WA-%' then 'Washington'
      else 'Other'
    end as old_region,
    case when dim.is_warranty_order = true then "Warranty" 
      when completed_at is not null then "Completed"
      when won_at is not null and dim.cancelled_at is not null and dim.cancelled_at >= won_at then "Cancelled - Won" 
      when won_at is not null then "Won" 
      when quoted_at is not null and dim.cancelled_at is not null and dim.cancelled_at >= quoted_at then "Cancelled - Quoted" 
      when quoted_at is not null then "Quoted"
      when dim.is_lead = true and dim.cancelled_at is not null then "Cancelled - Lead" 
      when dim.is_lead = true then "Lead"
      when dim.is_lead = false and dim.cancelled_at is not null then "Cancelled - Order"  
      else "Order" 
    end as order_status,
    case 
      when dim.product like '%Fence Installation%' then 105
      when dim.product like '%Driveway Installation%' then 34
      when dim.product like '%Landscaping%' then 132
    end as product_id,
    case 
      when if(parent_order_id is null,dim.channel,parent_order_dim.channel) like '%Home Advisor%' --if channel is Home Advisor
        then coalesce(
                    if(parent_order_id is null,dim.ha_type,parent_order_dim.ha_type)||if(parent_order_id is null,dim.channel,parent_order_dim.channel), --HA type (Commercial, Residential) + channel (HA,Yelp,Google,etc.)
                    if(parent_order_id is null,dim.lead_channel,parent_order_dim.lead_channel)||if(parent_order_id is null,dim.channel,parent_order_dim.channel), -- Type of lead (Commercial, Residential) + channel (HA,Yelp,Google,etc.)
                    '/Unknown') --if channel is unknown
        else coalesce( --if channel is not Home Advisor
                    if(parent_order_id is null,dim.lead_channel,parent_order_dim.lead_channel)||if(parent_order_id is null,dim.channel,parent_order_dim.channel) -- Type of lead (Commercial, Residential) + channel (HA,Yelp,Google,etc.)
                    ,'/Unknown') --if channel is unknown
    end as channel,
    if(parent_order_id is null,dim.ha_type,parent_order_dim.ha_type) as ha_type,
    case
      when if(parent_order_id is null,dim.channel,parent_order_dim.channel) like '%/Home Advisor/Ads%' then 'Home Advisor Ads'
      when if(parent_order_id is null,dim.channel,parent_order_dim.channel) like '%Home Advisor%' then 'Home Advisor'
      when if(parent_order_id is null,dim.channel,parent_order_dim.channel) like '%/Paid/Facebook%' then 'Paid/Facebook'
      when if(parent_order_id is null,dim.channel,parent_order_dim.channel) like '%/Non Paid/Facebook%' then 'Non Paid/Facebook'
      when if(parent_order_id is null,dim.channel,parent_order_dim.channel) like '%Thumbtack%' then 'Thumbtack'
      when if(parent_order_id is null,dim.channel,parent_order_dim.channel) like '%/Paid/Google%' then 'Paid/Google'
      when if(parent_order_id is null,dim.channel,parent_order_dim.channel) like '%/Non Paid/Google%' then 'Non Paid/Google'
      when if(parent_order_id is null,dim.channel,parent_order_dim.channel) like '%Yelp%' then 'Yelp'
      when if(parent_order_id is null,dim.channel,parent_order_dim.channel) like '%Nextdoor%' then 'Nextdoor'
      when if(parent_order_id is null,dim.channel,parent_order_dim.channel) like '%Bark%' then 'Bark'
      when if(parent_order_id is null,dim.channel,parent_order_dim.channel) like '%Borg%' then 'Borg'
      when if(parent_order_id is null,dim.channel,parent_order_dim.channel) like '%Non Paid/Direct%' then 'Non Paid/Direct'
      when if(parent_order_id is null,dim.channel,parent_order_dim.channel) like '%/Paid/%' then 'Paid/Misc'
      when if(parent_order_id is null,dim.channel,parent_order_dim.channel) like '%/Non Paid/%' then 'Non Paid/Misc'
    end as grouped_channel,
    case 
	    when if(won_at is not null,first_approved_price,first_quoted_price) between 1 and 1000 then 'A. 0.0K - 1K' 
	    when if(won_at is not null,first_approved_price,first_quoted_price)  between 1001 and 1500 then 'B. 1.0K - 1.5K'
      when if(won_at is not null,first_approved_price,first_quoted_price)  between 1501 and 2000 then 'C. 1.5K - 2.0K'
	    when if(won_at is not null,first_approved_price,first_quoted_price)  between 2001 and 2500 then 'D. 2.0K - 2.5K'
	    when if(won_at is not null,first_approved_price,first_quoted_price)  between 2501 and 3000 then 'E. 2.5K - 3.0K'
	    when if(won_at is not null,first_approved_price,first_quoted_price)  between 3001 and 4000 then 'F. 3.0K - 4.0K'
	    when if(won_at is not null,first_approved_price,first_quoted_price)  between 4001 and 5000 then 'G. 4.0K - 5.0K'
	    when if(won_at is not null,first_approved_price,first_quoted_price)  between 5001 and 6000 then 'H. 5.0K - 6.0K'
	    when if(won_at is not null,first_approved_price,first_quoted_price)  between 6001 and 8000 then 'I. 6.0K - 8.0K'
	    when if(won_at is not null,first_approved_price,first_quoted_price)  between 8001 and 10000 then 'J. 8.0K - 10.0K'
	    when if(won_at is not null,first_approved_price,first_quoted_price)  between 10001 and 12000 then 'K. 10.0K - 12.0K'
	    when if(won_at is not null,first_approved_price,first_quoted_price)  between 12001 and 15000 then 'L. 12.0K - 15.0K'
      when if(won_at is not null,first_approved_price,first_quoted_price)  between 15001 and 20000 then 'M. 15.0K - 20.0K'
      when if(won_at is not null,first_approved_price,first_quoted_price)  between 20001 and 25000 then 'N. 20.0K - 25.0K'
      when if(won_at is not null,first_approved_price,first_quoted_price)  between 25001 and 30000 then 'O. 25.0K - 30.0K'
      when if(won_at is not null,first_approved_price,first_quoted_price)  between 30001 and 40000 then 'P. 30.0K - 40.0K'
      when if(won_at is not null,first_approved_price,first_quoted_price)  between 40001 and 50000 then 'Q. 40.0K - 50.0K'
      when if(won_at is not null,first_approved_price,first_quoted_price)  >50000 then 'R. 50.0K - ...'
    end as Avg_Size_bucket,
    case 
	   when 1.0*if(won_at is not null,last_approved_mktg_discount,first_quoted_mktg_discount)/nullif(if(won_at is not null,last_approved_price,first_quoted_price),0) between 0.000001 and 0.01 then 'A. 0-1%' 
	   when 1.0*if(won_at is not null,last_approved_mktg_discount,first_quoted_mktg_discount)/nullif(if(won_at is not null,last_approved_price,first_quoted_price),0) between 0.01 and 0.02 then 'B. 1-2%' 
	   when 1.0*if(won_at is not null,last_approved_mktg_discount,first_quoted_mktg_discount)/nullif(if(won_at is not null,last_approved_price,first_quoted_price),0) between 0.02 and 0.03 then 'C. 2-3%' 
	   when 1.0*if(won_at is not null,last_approved_mktg_discount,first_quoted_mktg_discount)/nullif(if(won_at is not null,last_approved_price,first_quoted_price),0) between 0.03 and 0.04 then 'D. 3-4%' 
	   when 1.0*if(won_at is not null,last_approved_mktg_discount,first_quoted_mktg_discount)/nullif(if(won_at is not null,last_approved_price,first_quoted_price),0) between 0.04 and 0.05 then 'E. 4-5%' 
	   when 1.0*if(won_at is not null,last_approved_mktg_discount,first_quoted_mktg_discount)/nullif(if(won_at is not null,last_approved_price,first_quoted_price),0) between 0.05 and 0.07 then 'F. 5-7%' 
	   when 1.0*if(won_at is not null,last_approved_mktg_discount,first_quoted_mktg_discount)/nullif(if(won_at is not null,last_approved_price,first_quoted_price),0) between 0.07 and 0.1 then 'G. 7-10%' 
	   when 1.0*if(won_at is not null,last_approved_mktg_discount,first_quoted_mktg_discount)/nullif(if(won_at is not null,last_approved_price,first_quoted_price),0) > 0.1 then 'H. 10%+' 
     else '0. No Coupon'
    end as coupon_bucket,
    coalesce(cs_fee,0) + coalesce(csr_fee,0) + coalesce(quoter_fee,0) as sales_oh_cost,
    coalesce(pm_fee,0) as delivery_oh_cost,
    coalesce(sales_commission_fee,0) + coalesce(sales_photographer_fee,0) + coalesce(sales_review_fee,0) 
      + coalesce(ben_cs_fee,0) + coalesce(ben_csr_fee,0) + coalesce(ben_quoter_fee,0) as sales_var_cost,
    coalesce(delivery_review_fee,0) + coalesce(ben_pm_fee,0)as delivery_var_cost,
    coalesce(mktg_fee,0) + coalesce(warranty_pay,0) as net_costs,
    if(date_diff(current_date('America/Los_Angeles'),dim.created_at,day) >=360,1,0) as is_order360,
    if(date_diff(quoted_at,dim.created_at,day)<=360,1,0) as is_quoted360,
    if(date_diff(current_date('America/Los_Angeles'),won_at,day) >= 360,1,0) as is_win360,
    if(DATE_DIFF(completed_at,won_at,day) <= 360,1,0) as is_completed360w,
    if(date_diff(current_date('America/Los_Angeles'),dim.created_at,day) >=240,1,0) as is_order240,
    if(DATE_DIFF(won_at,dim.created_at,day) <= 360,1,0) as is_won360,
    if(DATE_DIFF(won_at,dim.created_at,day) <= 240,1,0) as is_won240,
    if(date_diff(quoted_at,dim.created_at,day)<=240,1,0) as is_quoted240,
    if(dim.is_lead=true and date_diff(current_date('America/Los_Angeles'),dim.created_at,day) >=180,1,0) as is_lead180,
    if(date_diff(current_date('America/Los_Angeles'),dim.created_at,day) >=180,1,0) as is_order180,
    if(DATE_DIFF(won_at,dim.created_at,day) <= 180,1,0) as is_won180,
    if(date_diff(current_date('America/Los_Angeles'),quoted_at,day) >=180,1,0) as is_quote180,
    if(date_diff(quoted_at,dim.created_at,day)<=180,1,0) as is_quoted180,
    if(DATE_DIFF(won_at,quoted_at,day) <= 180,1,0) as is_won180q,
    if(date_diff(current_date('America/Los_Angeles'),won_at,day) >= 180,1,0) as is_win180,
    if(DATE_DIFF(completed_at,won_at,day) <= 180,1,0) as is_completed180w,
    if(DATE_DIFF(completed_at,won_at,day) <= 150,1,0) as is_completed150w,
    if(date_diff(current_date('America/Los_Angeles'),won_at,day) >= 150,1,0) as is_win150,
    if(DATE_DIFF(completed_at,won_at,day) <= 120,1,0) as is_completed120w,
    if(date_diff(current_date('America/Los_Angeles'),won_at,day) >= 120,1,0) as is_win120,
    if(DATE_DIFF(completed_at,dim.created_at,day) <= 120,1,0) as is_completed120,
    if(dim.is_lead=true and date_diff(current_date('America/Los_Angeles'),dim.created_at,day) >=120,1,0) as is_lead120,
    if(date_diff(current_date('America/Los_Angeles'),dim.created_at,day) >=120,1,0) as is_order120,
    if(date_diff(current_date('America/Los_Angeles'),quoted_at,day) >=120,1,0) as is_quote120,
    if(date_diff(quoted_at,dim.created_at,day)<=120,1,0) as is_quoted120,
    if(DATE_DIFF(won_at,dim.created_at,day) <= 120,1,0) as is_won120,
    if(DATE_DIFF(won_at,quoted_at,day) <= 120,1,0) as is_won120q,
    if(DATE_DIFF(completed_at,won_at,day) <= 90,1,0) as is_completed90w,
    if(date_diff(current_date('America/Los_Angeles'),won_at,day) >= 90,1,0) as is_win90,
    if(DATE_DIFF(completed_at,dim.created_at,day) <= 90,1,0) as is_completed90,
    if(dim.is_lead=true and date_diff(current_date('America/Los_Angeles'),dim.created_at,day) >=90,1,0) as is_lead90,
    if(date_diff(current_date('America/Los_Angeles'),dim.created_at,day) >=90,1,0) as is_order90,
    if(date_diff(current_date('America/Los_Angeles'),quoted_at,day) >=90,1,0) as is_quote90,
    if(date_diff(quoted_at,dim.created_at,day)<=90,1,0) as is_quoted90,
    if(DATE_DIFF(won_at,dim.created_at,day) <= 90,1,0) as is_won90,
    if(DATE_DIFF(won_at,quoted_at,day) <= 90,1,0) as is_won90q,
    if(DATE_DIFF(completed_at,won_at,day) <= 60,1,0) as is_completed60w,
    if(date_diff(current_date('America/Los_Angeles'),won_at,day) >= 60,1,0) as is_win60,
    if(DATE_DIFF(completed_at,dim.created_at,day) <= 60,1,0) as is_completed60,
    if(dim.is_lead=true and date_diff(current_date('America/Los_Angeles'),dim.created_at,day) >=60,1,0) as is_lead60,
    if(date_diff(current_date('America/Los_Angeles'),dim.created_at,day) >=60,1,0) as is_order60,
    if(date_diff(current_date('America/Los_Angeles'),quoted_at,day) >=60,1,0) as is_quote60,
    if(date_diff(quoted_at,dim.created_at,day)<=60,1,0) as is_quoted60,
    if(DATE_DIFF(won_at,dim.created_at,day) <= 60,1,0) as is_won60,
    if(DATE_DIFF(won_at,quoted_at,day) <= 60,1,0) as is_won60q,
    if(DATE_DIFF(completed_at,won_at,day) <= 30,1,0) as is_completed30w,
    if(date_diff(current_date('America/Los_Angeles'),won_at,day) >= 30,1,0) as is_win30,
    if(DATE_DIFF(completed_at,dim.created_at,day) <= 30,1,0) as is_completed30,
    if(dim.is_lead=true and date_diff(current_date('America/Los_Angeles'),dim.created_at,day) >=30,1,0) as is_lead30,
    if(date_diff(current_date('America/Los_Angeles'),dim.created_at,day) >=30,1,0) as is_order30,
    if(date_diff(current_date('America/Los_Angeles'),quoted_at,day) >=30,1,0) as is_quote30,
    if(date_diff(quoted_at,dim.created_at,day)<=30,1,0) as is_quoted30,
    if(DATE_DIFF(won_at,dim.created_at,day) <= 30,1,0) as is_won30,
    if(DATE_DIFF(won_at,quoted_at,day) <= 30,1,0) as is_won30q,
    if(DATE_DIFF(completed_at,dim.created_at,day) <= 14,1,0) as is_completed14,
    if(dim.is_lead=true and date_diff(current_date('America/Los_Angeles'),dim.created_at,day) >=14,1,0) as is_lead14,
    if(date_diff(current_date('America/Los_Angeles'),dim.created_at,day) >=14,1,0) as is_order14,
    if(date_diff(booked_ts_at,dim.created_ts_at,day)<=14,1,0) as is_booked14,
    if(date_diff(current_date('America/Los_Angeles'),booked_ts_at,day) >=14,1,0) as is_booke14,
    if(DATE_DIFF(onsite_ts_at,booked_ts_at,day) <= 14,1,0) as is_onsited14b,
    if(date_diff(current_date('America/Los_Angeles'),onsite_ts_at,day) >=14,1,0) as is_onsite14,
    if(DATE_DIFF(quoted_at,onsite_ts_at,day) <= 14,1,0) as is_quote14ons,
    if(date_diff(current_date('America/Los_Angeles'),quoted_at,day) >=14,1,0) as is_quote14,
    if(date_diff(quoted_at,dim.created_at,day)<=14,1,0) as is_quoted14,
    if(DATE_DIFF(won_at,dim.created_at,day) <= 14,1,0) as is_won14,
    if(DATE_DIFF(won_at,quoted_at,day) <= 14,1,0) as is_won14q,
    if(dim.is_lead=true and date_diff(current_date('America/Los_Angeles'),dim.created_at,day) >=7,1,0) as is_lead7,
    if(date_diff(current_date('America/Los_Angeles'),dim.created_at,day) >=7,1,0) as is_order7,
    if(date_diff(current_date('America/Los_Angeles'),quoted_at,day) >=7,1,0) as is_quote7,
    if(date_diff(quoted_at,dim.created_at,day)<=7,1,0) as is_quoted7,
    if(DATE_DIFF(won_at,dim.created_at,day) <= 7,1,0) as is_won7,
    if(DATE_DIFF(won_at,quoted_at,day) <= 7,1,0) as is_won7q,
    if(date_diff(booked_ts_at,dim.created_ts_at,day)<=7,1,0) as is_booked7,
    if(date_diff(current_date('America/Los_Angeles'),booked_ts_at,day) >=7,1,0) as is_booke7,
    if(DATE_DIFF(onsite_ts_at,booked_ts_at,day) <= 7,1,0) as is_onsited7b,
    if(date_diff(current_date('America/Los_Angeles'),onsite_ts_at,day) >=7,1,0) as is_onsite7,
    if(date_diff(onsite_ts_at,dim.created_ts_at,day) <=7,1,0) as is_onsited7,
    DATETime_DIFF(completed_ts_at,dim.created_ts_at,hour)/24 as tat_ad,
    DATEtime_DIFF(booked_ts_at,dim.created_ts_at,hour)/24 as tat_ab,
    DATEtime_DIFF(onsite_ts_at,dim.created_ts_at,hour)/24 as tat_ao,
    DATEtime_DIFF(quoted_ts_at,dim.created_ts_at,hour)/24 as tat_aq,
    DATEtime_DIFF(quote_requested_ts_at, dim.created_ts_at,hour)/24 as tat_ar,
    case when dim.cancelled_ts_at >= quoted_ts_at then DATEtime_DIFF(dim.cancelled_ts_at,quoted_ts_at,hour)/24 end as tat_qc,
    DATEtime_DIFF(dim.cancelled_ts_at,dim.created_ts_at,hour)/24 as tat_ac,
    DATEtime_DIFF(won_ts_at, dim.created_ts_at,hour)/24 as tat_aw,
    DATEtime_DIFF(won_ts_at, quoted_ts_at,hour)/24 as tat_qw,
    DATETime_DIFF(paid_at,completed_at,hour)/24 as tat_dp,
    DATETime_DIFF(quoted_ts_at,quote_requested_ts_at,hour)/24 as tat_rq,
    DATETime_DIFF(completed_ts_at,won_ts_at,hour)/24 as tat_wd,
    DATETime_DIFF(onsite_ts_at,booked_ts_at,hour)/24 as tat_bo,
    DATETime_DIFF(quoted_ts_at,onsite_ts_at,hour)/24 as tat_oq,
    DATEtime_DIFF(estimated_ts_at,onsite_ts_at,hour)/24 as tat_oe,
    if(estimated_ts_at < quoted_ts_at,DATETime_DIFF(quoted_ts_at,estimated_ts_at,hour)/24,null) as tat_eq,
    DATETime_DIFF(dim.scoping_task_at,won_ts_at,hour)/24 as tat_ws,
    DATETime_DIFF(install_planned_ts_at,won_ts_at,hour)/24 as tat_wip,
    DATETime_DIFF(install_booked_ts_at,won_ts_at,hour)/24 as tat_wib,
    DATETime_DIFF(install_planned_ts_at,install_booked_ts_at,hour)/24 as tat_ibp,
    DATETime_DIFF(last_install_planned_ts_at,install_planned_ts_at,hour)/24 as tat_ipli,
    DATETime_DIFF(completed_ts_at,last_install_planned_ts_at,hour)/24 as tat_lipd,
    DATETime_DIFF(completed_ts_at,install_planned_ts_at,hour)/24 as tat_ipd,
    if(dim.cancelled_at is not null,1,0) as is_cancelled,
    if(completed_at is not null,1,0) as is_completed,
    if(won_at is not null,1,0) as is_win,
    if(quoted_at is not null,1,0) as is_quoted,
    if(onsite_ts_at is not null,1,0) as is_onsite,
    if(booked_ts_at is not null,1,0) as is_booked,
    if(estimated_ts_at is not null,1,0) as is_estimate,
    if(install_planned_ts_at is not null,1,0) as is_install_planned,
    if(photographer_visits>0,'physical', if(onsite_ts_at is not null, 'remote',if(quoted_at is not null,'no onsite','not quoted'))) as onsite_type,
    coalesce(completed_at,dim.cancelled_at) as closed_at,coalesce(won_at,dim.cancelled_at) as closedW_at,
    (last_approved_mktg_discount>0) as is_couponed, if(paid_at is not null,1,0) as is_paid,
    is_trailing_period(dim.created_at,7)   as created_t7days,
    is_trailing_period(dim.created_at,14)  as created_t14days,
    is_trailing_period(dim.created_at,28)  as created_t28days,
    is_trailing_period(dim.created_at,56)  as created_t56days,
    is_trailing_period(dim.created_at,84)  as created_t84days,
    is_trailing_period(dim.created_at,112) as created_t112days,
    is_trailing_period(quoted_at,7)   as quoted_t7days,
    is_trailing_period(quoted_at,14)  as quoted_t14days,
    is_trailing_period(quoted_at,28)  as quoted_t28days,
    is_trailing_period(quoted_at,56)  as quoted_t56days,
    is_trailing_period(quoted_at,84)  as quoted_t84days,
    is_trailing_period(quoted_at,112) as quoted_t112days,
    is_trailing_period(won_at,7)   as won_t7days,
    is_trailing_period(won_at,14)  as won_t14days,
    is_trailing_period(won_at,28)  as won_t28days,
    is_trailing_period(won_at,56)  as won_t56days,
    is_trailing_period(won_at,84)  as won_t84days,
    is_trailing_period(won_at,112) as won_t112days,
    is_trailing_period(coalesce(won_at,dim.cancelled_at),7)   as closedW_t7days,
    is_trailing_period(coalesce(won_at,dim.cancelled_at),14)  as closedW_t14days,
    is_trailing_period(coalesce(won_at,dim.cancelled_at),28)  as closedW_t28days,
    is_trailing_period(coalesce(won_at,dim.cancelled_at),56)  as closedW_t56days,
    is_trailing_period(coalesce(won_at,dim.cancelled_at),84)  as closedW_t84days,
    is_trailing_period(coalesce(won_at,dim.cancelled_at),112) as closedW_t112days,
    is_trailing_period(coalesce(completed_at,dim.cancelled_at),7)   as closed_t7days,
    is_trailing_period(coalesce(completed_at,dim.cancelled_at),14)  as closed_t14days,
    is_trailing_period(coalesce(completed_at,dim.cancelled_at),28)  as closed_t28days,
    is_trailing_period(coalesce(completed_at,dim.cancelled_at),56)  as closed_t56days,
    is_trailing_period(coalesce(completed_at,dim.cancelled_at),84)  as closed_t84days,
    is_trailing_period(coalesce(completed_at,dim.cancelled_at),112) as closed_t112days
  from int_data.order_dimensions dim
  left join int_data.order_mktg_fee mk on mk.order_id = dim.order_id
  left join int_data.order_allocated_overhead al on al.order_id = dim.order_id
  left join int_data.order_attributed_quote aq on aq.order_id = dim.order_id
  left join int_data.order_attributed_overhead att on att.order_id = dim.order_id
  left join int_data.order_contractor_pay co on co.order_id = dim.order_id
  left join int_data.order_feedback f on f.order_id = dim.order_id
  left join int_data.order_yelp y on y.order_id = dim.order_id
  left join int_data.order_dimensions parent_order_dim on parent_order_dim.order_id = co.parent_order_id
),

orders as (
  --Check this link to find the source of each column https://docs.google.com/spreadsheets/d/19yofiRFPbgtkYnzBWw7AzcsVvGGnplmUkrBjKdYGubI/edit#gid=0 
  select
    dim.order_id,
    dim.lead_id,
    is_lead,
    dim.created_at,created_ts_at,quoted_at,quoted_ts_at,quote_requested_ts_at, 
    won_at,won_ts_at,completed_at,completed_ts_at,marked_completed_at, warranty_at, 
    cancelled_at,cancelled_ts_at, photographer_visit_at,paid_at,onsite_ts_at,
    estimated_ts_at,booked_ts_at,scoping_task_at, install_planned_ts_at, last_install_planned_ts_at,
    order_status,
    ed.channel,
    ed.ha_type,
    coalesce(dim.channel1,'/Unknown') as channel1, --based on customer
    grouped_channel,
    coalesce(geo,'/Unknown') as geo,
    ed.segment,
    coalesce(dim.product,'/Unknown') as product,
    coalesce(dim.product_quoted,'/Unknown') as product_quoted, 
    coalesce(type, '/Unknown') as type,
    coalesce(tier,'Unknown') as tier,
    coalesce(ha_initial_fee,0) as ha_initial_fee,
    coalesce(ha_fee,0) as ha_fee,
    ha_refund_at,
    coalesce(fb_fee,0) as fb_fee,
    coalesce(tt_fee,0) as tt_fee,
    coalesce(nd_fee,0) as nd_fee,
    coalesce(gg_fee,0) as gg_fee,
    coalesce(gg_gls_fee,0) as gg_gls_fee,
    coalesce(bo_fee,0) as bo_fee,
    coalesce(ba_fee,0) as ba_fee,
    coalesce(sdr_fee,0) as sdr_fee,
    coalesce(ha_ads_fee,0) as ha_ads_fee,
    coalesce(mktg_fee,0) + coalesce(yelp_cpl_budget,0) as mktg_fee,
    coalesce(cs_fee,0) as cs_fee,
    coalesce(csr_fee,0) as csr_fee,
    coalesce(quoter_fee,0) as quoter_fee,
    coalesce(pm_fee,0) as pm_fee,
    coalesce(ben_cs_fee,0) as ben_cs_fee,
    coalesce(ben_csr_fee,0) as ben_csr_fee,
    coalesce(ben_quoter_fee,0) as ben_quoter_fee,
    coalesce(ben_pm_fee,0) as ben_pm_fee,
    coalesce(sales_commission_fee,0) as sales_commission_fee,
    coalesce(photographer_visits,0) as photographer_visits,
    coalesce(photographer_visits_post_won,0) as photographer_visits_post_won,
    coalesce(sales_photographer_fee,0) as sales_photographer_fee,
    coalesce(sales_review_fee,0) as sales_review_fee,
    coalesce(delivery_review_fee,0) as delivery_review_fee,
    coalesce(handyman_fee,0) as handyman_fee,
    coalesce(contractor_pay,0) as contractor_pay,
    coalesce(installer_pay,0) as installer_pay,
    coalesce(revenue,0) as revenue,
    coalesce(first_quoted_price,0) as first_quoted_price,
    coalesce(first_quoted_cost,0) as first_quoted_cost,
    coalesce(first_approved_price,0) as first_approved_price,
    coalesce(first_approved_cost,0) as first_approved_cost,
    coalesce(last_approved_price,0) as last_approved_price,
    coalesce(last_approved_cost,0) as last_approved_cost,
    coalesce(last_quoted_price,0) as last_quoted_price,
    coalesce(last_quoted_cost,0) as last_quoted_cost,
    coalesce(first_quoted_sales_discount,0) as first_quoted_sales_discount,
    coalesce(first_quoted_mktg_discount,0) as first_quoted_mktg_discount,
    coalesce(first_approved_sales_discount,0) as first_approved_sales_discount, 
    coalesce(first_approved_mktg_discount,0) as first_approved_mktg_discount,
    coalesce(first_approved_delivery_discount,0) as first_approved_delivery_discount,
    coalesce(first_approved_pricing_discount,0) as first_approved_pricing_discount,
    coalesce(last_approved_sales_discount,0) as last_approved_sales_discount, 
    coalesce(last_approved_mktg_discount,0) as last_approved_mktg_discount,
    coalesce(last_approved_delivery_discount,0) as last_approved_delivery_discount,
    coalesce(last_approved_pricing_discount,0) as last_approved_pricing_discount,
    coalesce(last_approved_supply_discount,0) as last_approved_supply_discount,
    coalesce(collected_cc_fees,0) as collected_cc_fees,
    coalesce(paid_cc_fees,0) as paid_cc_fees,
    coalesce(credit_card_fees,0) as credit_card_fees,
    coalesce(bank_transfer_fees, 0) as bank_transfer_fees,
    coalesce(wisetack_fees, 0) as wisetack_fees,
    coalesce(warranty_estimate,0) as warranty_estimate,
    coalesce(warranty_pay,0) as warranty_pay,
    coalesce(materials_pay,0) as materials_pay,
    coalesce(materials_deduction,0) as materials_deduction,
    coalesce(finance_disc,0) as finance_disc,
    coalesce(wwo_installer_leakage,0) as wwo_installer_leakage,
    coalesce(cost_of_sales,0) as cost_of_sales,
    coalesce(gp,0) as gp,
    coalesce(wwo_count,0) as wwo_count,
    coalesce(wwo_completed_count) as wwo_completed_count,
    coalesce(gp,0)+coalesce(cost_of_sales,0) as nr,  
    coalesce(cogs,0) as cogs,
    coalesce(sales_var_cost+sales_oh_cost,0) as sales_cost,
    coalesce(delivery_var_cost+delivery_oh_cost,0) as delivery_cost,
    coalesce(sales_oh_cost,0) as sales_oh_cost,
    coalesce(delivery_oh_cost,0) as delivery_oh_cost,
    coalesce(sales_var_cost,0) as sales_var_cost,
    coalesce(delivery_var_cost,0) as delivery_var_cost,
    coalesce(sales_oh_cost+delivery_oh_cost,0) as overhead_cost,
    coalesce(gp-(sales_var_cost+delivery_var_cost+sales_oh_cost+delivery_oh_cost),0) as cp,
    coalesce(gp-(sales_var_cost+delivery_var_cost),0) as vcp,
    coalesce(net_costs,0) as net_costs,
    coalesce((gp-(sales_var_cost+delivery_var_cost+sales_oh_cost+delivery_oh_cost))-net_costs-handyman_fee) as np,
    coalesce((gp-(sales_var_cost+delivery_var_cost))-net_costs-handyman_fee) as vnp,
    coalesce(cancelled_onsites,0) as cancelled_onsites,
    coalesce(completed_onsites,0) as completed_onsites,
    coalesce(no_show_onsites,0) as no_show_onsites,
    coalesce(bookings,0) as bookings,
    fb_yelp_posted_at,
    fb_thumbtack_posted_at,
    fb_bbb_posted_at,
    fb_google_posted_at,
    fb_ha_posted_at,
    fb_internal_posted_at,
    fb_yelp_score,
    fb_thumbtack_score,
    fb_bbb_score,
    fb_google_score,
    fb_ha_score,
    fb_ha_customer_service_score,
    fb_ha_value_for_money_score,
    fb_internal_score,
    fb_internal_communication_score,
    fb_internal_installation_score,
    fb_internal_scheduling_score,
    fb_internal_quoting_score,
    dim.lost_reason,
    dim.lost_reason_text,
    yelp_recommended,
    sales_rep, sales_team, sales_title,
    sales_staff_id,
    project_manager, pm_team,
    pm_id,
    contractor,
    contractor_count,
    quoter,quoted_dep,first_quoted_dept,last_quoted_dept,coalesce(delta,0) as delta,
    photographer,
    fb_attributed_dept,
    fb_sales_attributed,
    fb_delivery_attributed,
    fb_first_posted_at,
    has_escalation,
    msa,
    cmsa,
    dim.region,
    ed.old_region,
    dim.market,
    dim.market_id,
    county,
    city,
    state,
    yelp_location,
    google_location,
    ha_location,
    ROW_NUMBER() OVER (PARTITION BY dim.order_id ORDER BY dim.order_id) AS RN,
    dim.is_warranty_order,
    coalesce(last_approved_small_project_overhead,0) as last_approved_small_project_overhead,
    coalesce(quoted_small_project_overhead,0) as quoted_small_project_overhead,
    additional_mh_labor,
    round(coalesce(yelp_cpl_spend,0) ,2) as yelp_cpl_spend, 
    round(coalesce(yelp_cpl_budget,0),2) as yelp_cpl_budget,
    round(coalesce(yelp_cpm_spend,0) ,2) as yelp_cpm_spend, 
    round(coalesce(yelp_cpm_budget,0),2) as yelp_cpm_budget,
    dim.utm_medium as utm_medium,
    mpl,
    latitude,
    longitude,
    case when dim.product_quoted like '%Install a Wood Fence' or product_quoted like '%Vinyl%' or product_quoted like '%Chain Link%' then total_length else null end as total_length,
    case when dim.product_quoted like '%Install a Wood Fence' then gate_length else null end as gate_length,
    if(dim.product_quoted like '/Landscaping%',sqft,null) as sqft,
    coalesce(quotes_sent_count,0) as quotes_sent_count,
    coalesce(change_order_count,0) as change_order_count,
    coalesce(scoping_task_count,0) as scoping_task_count,
    coalesce(qa_process_count,0) as qa_process_count,
    coalesce(estimates,0) as estimates,
    coalesce(count_yelp_recommended,0) as count_yelp_recommended,coalesce(count_yelp_reviews,0) as count_yelp_reviews,
    coalesce(count_google_reviews,0) as count_google_reviews,
    coalesce(count_thumbtack_reviews,0) as count_thumbtack_reviews,
    coalesce(count_bbb_reviews,0) as count_bbb_reviews,
    coalesce(count_ha_reviews,0) as count_ha_reviews,
    merchant_fee_type,
    house_id,
    customer_id,
    contact_name,
    product_id, 
    first_quote_id,
    last_quote_id,
    first_approved_quote_id,
    case 
      when won_at is not null and multi_party_approval is null then 'no'
      else multi_party_approval
    end as multi_party_approval,
    is_draft_editor,is_booking_cancelled,is_booking_no_show,
    is_won360,is_completed360w,is_win360,is_quoted360,is_order360,
    is_quoted240,is_won240,is_order240,
    is_lead180,is_order180,is_won180,is_quote180,is_quoted180,is_won180q,is_win180,is_completed180w,
    is_completed150w,is_win150,
    is_completed120w,is_win120,is_completed120,is_lead120,is_order120,is_quote120,is_quoted120,is_won120,is_won120q,
    is_completed90w,is_win90,is_completed90,is_lead90,is_order90,is_quote90,is_quoted90,is_won90,is_won90q,
    is_completed60w,is_win60,is_completed60,is_lead60,is_order60,is_quote60,is_quoted60,is_won60,is_won60q,
    is_completed30w,is_win30,is_completed30,is_lead30,is_order30,is_quote30,is_quoted30,is_won30,is_won30q,
    is_won14,is_completed14,is_lead14,is_order14,is_quote14,is_quoted14,is_won14q,is_booked14,is_booke14,is_onsited14b,is_onsite14,is_quote14ons,
    is_lead7,is_order7,is_quote7,is_quoted7,is_won7q,is_won7,is_booked7,is_booke7,is_onsited7b,is_onsite7,is_onsited7,
    segment_l1,
    tat_ad,tat_aq,tat_ar,tat_aw,tat_dp,tat_rq,tat_wd,tat_qw,tat_ac,tat_qc,tat_oe,tat_eq,tat_bo,tat_oq,tat_ab,tat_ao,tat_ws,tat_wip,tat_ibp,tat_wib,tat_ipli,tat_lipd,tat_ipd,
    is_cancelled,is_completed,is_win,is_quoted,is_onsite,is_booked,is_estimate,is_install_planned,
    onsite_type,avg_size_bucket,is_couponed,is_paid,coupon_bucket,
    closed_at,closedW_at,
    created_t7days,created_t14days,created_t28days,created_t56days,created_t84days,created_t112days,
    quoted_t7days,quoted_t14days,quoted_t28days,quoted_t56days,quoted_t84days,quoted_t112days,
    won_t7days,won_t14days,won_t28days,won_t56days,won_t84days,won_t112days,
    closedW_t7days,closedW_t14days,closedW_t28days,closedW_t56days,closedW_t84days,closedW_t112days,
    closed_t7days,closed_t14days,closed_t28days,closed_t56days,closed_t84days,closed_t112days,
    zip_admin_id,
    zipcode_id,
    zip_median_income,
    zip_median_age,
    zip_median_house_value,
    zip_median_house_age,
    zip_housing_units,
    zip_total_population,
    zip_family_households,
    dim.other_pros_matched,
    ms.volume as ms_ha_leads,
    coalesce(is_waiver,0) as is_waiver,
    parent_order_id
  from int_data.order_dimensions dim
  left join int_data.order_mktg_fee mk on mk.order_id = dim.order_id
  left join int_data.order_allocated_overhead al on al.order_id = dim.order_id
  left join int_data.order_attributed_quote aq on aq.order_id = dim.order_id
  left join int_data.order_attributed_overhead att on att.order_id = dim.order_id
  left join int_data.order_contractor_pay co on co.order_id = dim.order_id
  left join int_data.order_feedback f on f.order_id = dim.order_id
  left join int_data.order_yelp y on y.order_id = dim.order_id
  left join extra_dimensions ed on ed.order_id = dim.order_id
  left join int_data.market_share_query ms on ms.order_id = ed.order_id
)

select 
  * except(RN)
from orders
where created_at  > Date('2018-04-15') 
  and RN = 1
