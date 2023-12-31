select
         day as date,
         mar188,
         MAR284,
         MAR285,
         MAR286,
         MAR287,
         MAR288,
         MAR289,
         MAR290,
         MAR291,
         MAR934,
         MAR935,
         MAR936,
         MAR274,
         MAR275,
         MAR276,
         MAR277,
         MAR278,
         MAR279,
         MAR280,
         MAR281,
         MAR282,
         MAR342,
         MAR343,
         MAR574,
         MAR575,
         MAR576,
         MAR580,
         MAR581,
         MAR582,
         MAR712,
         MAR928,
         MAR929,
         MAR930,
         MAR713,
         MAR1027,
         MAR1061,
         coalesce(MAR188/nullif(all_reviews,0),0) as MAR292,
         coalesce(MAR284/nullif(all_reviews_paloalto,0),0) as MAR293,
         coalesce(MAR285/nullif(all_reviews_watsonville,0),0) as MAR294,
         coalesce(MAR286/nullif(all_reviews_sacramento,0),0) as MAR295,
         coalesce(MAR287/nullif(all_reviews_stockton,0),0) as MAR296,
         coalesce(MAR288/nullif(all_reviews_oakland,0),0) as MAR297,
         coalesce(MAR289/nullif(all_reviews_fresno,0),0) as MAR298,
         coalesce(MAR290/nullif(all_reviews_san_jose,0),0) as MAR299,
         coalesce(MAR291/nullif(all_reviews_napa,0),0) as MAR300,
         coalesce(MAR343/nullif(all_reviews_san_francisco,0),0) as MAR344,
         coalesce(MAR574/nullif(all_reviews_riverside,0),0) as MAR577,
         coalesce(MAR575/nullif(all_reviews_lake_forest,0),0) as MAR578,
         coalesce(MAR576/nullif(all_reviews_thousand_oaks,0),0) as MAR579,
         coalesce(MAR713/nullif(all_reviews_los_angeles,0),0) as MAR714,
         coalesce(MAR934/nullif(all_reviews_san_diego,0),0) as MAR931,
         coalesce(MAR935/nullif(all_reviews_dallas,0),0) as MAR932,
         coalesce(MAR936/nullif(all_reviews_fort_worth,0),0) as MAR933,
         coalesce(MAR1062/nullif(all_reviews_houston,0),0) as MAR1064,
         coalesce(MAR1109/nullif(all_reviews_san_antonio,0),0) as MAR1110,
         coalesce(MAR1103/nullif(all_reviews_austin,0),0) as MAR1104,
         coalesce(MAR1063/nullif(all_reviews_atlanta,0),0) as MAR1065,
         coalesce(MAR1117/nullif(all_reviews_baltimore,0),0) as MAR1118,
         coalesce(MAR1158/nullif(all_reviews_maryland_dc,0),0) as MAR1159
         from
(
select
        *,
        rank() over (partition by day order by date desc) as rank
from
(
with calc_series as
(
    select
        generate_series('2018-04-15', current_date, '1 day')::date as day
   ),
yelp_data as
(
    select
        (posted_at at time zone 'pst')::date as date_id,
        count(fr.id) filter (where fr.is_yelp_recommended is true) as positive_reviews,
        count(fr.id) filter (where fr.is_yelp_recommended is true and mnl.label = 'Yelp - Palo Alto') as positive_reviews_palo_alto,
        count(fr.id) filter (where fr.is_yelp_recommended is true and mnl.label = 'Yelp - San Jose') as positive_reviews_san_jose,
        count(fr.id) filter (where fr.is_yelp_recommended is true and mnl.label = 'Yelp - Oakland') as positive_reviews_oakland,
        count(fr.id) filter (where fr.is_yelp_recommended is true and mnl.label = 'Yelp - San Francisco') as positive_reviews_san_francisco,
        count(fr.id) filter (where fr.is_yelp_recommended is true and mnl.label = 'Yelp - Sacramento') as positive_reviews_sacramento,
        count(fr.id) filter (where fr.is_yelp_recommended is true and mnl.label = 'Yelp - Napa') as positive_reviews_napa,
        count(fr.id) filter (where fr.is_yelp_recommended is true and mnl.label = 'Yelp - Watsonville') as positive_reviews_watsonville,
        count(fr.id) filter (where fr.is_yelp_recommended is true and mnl.label = 'Yelp - Fresno') as positive_reviews_fresno,
        count(fr.id) filter (where fr.is_yelp_recommended is true and mnl.label = 'Yelp - Stockton') as positive_reviews_stockton,
        count(fr.id) filter (where fr.is_yelp_recommended is true and mnl.label = 'Yelp - Riverside') as positive_reviews_riverside,
        count(fr.id) filter (where fr.is_yelp_recommended is true and mnl.label = 'Yelp - Lake Forest') as positive_reviews_lake_forest,
        count(fr.id) filter (where fr.is_yelp_recommended is true and mnl.label = 'Yelp - Thousand Oaks') as positive_reviews_thousand_oaks,
        count(fr.id) filter (where fr.is_yelp_recommended is true and mnl.label = 'Yelp - Los Angeles') as positive_reviews_los_angeles,
        count(fr.id) filter (where fr.is_yelp_recommended is true and mnl.label = 'Yelp - San Diego') as positive_reviews_san_diego,
        count(fr.id) filter (where fr.is_yelp_recommended is true and mnl.label = 'Yelp - Dallas') as positive_reviews_dallas,
        count(fr.id) filter (where fr.is_yelp_recommended is true and mnl.label = 'Yelp - Forth Worth') as positive_reviews_fort_worth,
        count(fr.id) filter (where fr.is_yelp_recommended is true and mnl.label = 'Yelp - Houston') as positive_reviews_houston,
        count(fr.id) filter (where fr.is_yelp_recommended is true and mnl.label = 'Yelp - San Antonio') as positive_reviews_san_antonio,
        count(fr.id) filter (where fr.is_yelp_recommended is true and mnl.label = 'Yelp - Austin') as positive_reviews_austin,
        count(fr.id) filter (where fr.is_yelp_recommended is true and mnl.label = 'Yelp - Atlanta') as positive_reviews_atlanta,
        count(fr.id) filter (where fr.is_yelp_recommended is true and mnl.label = 'Yelp - Baltimore') as positive_reviews_baltimore,
        count(fr.id) filter (where fr.is_yelp_recommended is true and mnl.label = 'Yelp - Maryland DC') as positive_reviews_maryland_dc,
        count(fr.id) as all_reviews,
        count(fr.id) filter (where mnl.label = 'Yelp - Palo Alto') as all_reviews_palo_alto,
        count(fr.id) filter (where mnl.label = 'Yelp - San Jose') as all_reviews_san_jose,
        count(fr.id) filter (where mnl.label = 'Yelp - Oakland') as all_reviews_oakland,
        count(fr.id) filter (where mnl.label = 'Yelp - San Francisco') as all_reviews_san_francisco,
        count(fr.id) filter (where mnl.label = 'Yelp - Sacramento') as all_reviews_sacramento,
        count(fr.id) filter (where mnl.label = 'Yelp - Napa') as all_reviews_napa,
        count(fr.id) filter (where mnl.label = 'Yelp - Watsonville') as all_reviews_watsonville,
        count(fr.id) filter (where mnl.label = 'Yelp - Fresno') as all_reviews_fresno,
        count(fr.id) filter (where mnl.label = 'Yelp - Stockton') as all_reviews_stockton,
        count(fr.id) filter (where mnl.label = 'Yelp - Riverside') as all_reviews_riverside,
        count(fr.id) filter (where mnl.label = 'Yelp - Lake Forest') as all_reviews_lake_forest,
        count(fr.id) filter (where mnl.label = 'Yelp - Thousand Oaks') as all_reviews_thousand_oaks,
        count(fr.id) filter (where mnl.label = 'Yelp - Los Angeles') as all_reviews_los_angeles,
        count(fr.id) filter (where mnl.label = 'Yelp - San Diego') as all_reviews_san_diego,
        count(fr.id) filter (where mnl.label = 'Yelp - Dallas') as all_reviews_dallas,
        count(fr.id) filter (where mnl.label = 'Yelp - Forth Worth') as all_reviews_fort_worth,
        count(fr.id) filter (where mnl.label = 'Yelp - Houston') as all_reviews_houston,
        count(fr.id) filter (where mnl.label = 'Yelp - San Antonio') as all_reviews_san_antonio,
        count(fr.id) filter (where mnl.label = 'Yelp - Austin') as all_reviews_austin,
        count(fr.id) filter (where mnl.label = 'Yelp - Atlanta') as all_reviews_atlanta,
        count(fr.id) filter (where mnl.label = 'Yelp - Baltimore') as all_reviews_baltimore,
        count(fr.id) filter (where mnl.label = 'Yelp - Maryland DC') as all_reviews_maryland_dc,
        count(fr.id) filter (where mnl.label = '') as all_reviews_no_location,
        avg(fr.score) filter (where fr.is_yelp_recommended is true) as avg_score,
        avg(fr.score) filter (where fr.is_yelp_recommended is true and mnl.label = 'Yelp - Palo Alto') as avg_score_palo_alto,
        avg(fr.score) filter (where fr.is_yelp_recommended is true and mnl.label = 'Yelp - San Jose') as avg_score_san_jose,
        avg(fr.score) filter (where fr.is_yelp_recommended is true and mnl.label = 'Yelp - Oakland') as avg_score_oakland,
        avg(fr.score) filter (where fr.is_yelp_recommended is true and mnl.label = 'Yelp - San Francisco') as avg_score_san_francisco,
        avg(fr.score) filter (where fr.is_yelp_recommended is true and mnl.label = 'Yelp - Sacramento') as avg_score_sacramento,
        avg(fr.score) filter (where fr.is_yelp_recommended is true and mnl.label = 'Yelp - Napa' ) as avg_score_napa,
        avg(fr.score) filter (where fr.is_yelp_recommended is true and mnl.label = 'Yelp - Watsonville') as avg_score_watsonville,
        avg(fr.score) filter (where fr.is_yelp_recommended is true and mnl.label = 'Yelp - Fresno') as avg_score_fresno,
        avg(fr.score) filter (where fr.is_yelp_recommended is true and mnl.label = 'Yelp - Stockton') as avg_score_stockton,
        avg(fr.score) filter (where fr.is_yelp_recommended is true and mnl.label = 'Yelp - Riverside') as avg_score_riverside,
        avg(fr.score) filter (where fr.is_yelp_recommended is true and mnl.label = 'Yelp - Lake Forest') as avg_score_lake_forest,
        avg(fr.score) filter (where fr.is_yelp_recommended is true and mnl.label = 'Yelp - Thousand Oaks') as avg_score_thousand_oaks,
        avg(fr.score) filter (where fr.is_yelp_recommended is true and mnl.label = 'Yelp - Los Angeles') as avg_score_los_angeles,
        avg(fr.score) filter (where fr.is_yelp_recommended is true and mnl.label = 'Yelp - San Diego') as avg_score_san_diego,
        avg(fr.score) filter (where fr.is_yelp_recommended is true and mnl.label = 'Yelp - Dallas') as avg_score_dallas,
        avg(fr.score) filter (where fr.is_yelp_recommended is true and mnl.label = 'Yelp - Fort Worth') as avg_score_fort_worth,
        avg(fr.score) filter (where fr.is_yelp_recommended is true and mnl.label = 'Yelp - Houston') as avg_score_houston,
        avg(fr.score) filter (where fr.is_yelp_recommended is true and mnl.label = 'Yelp - San Antonio') as avg_score_san_antonio,
        avg(fr.score) filter (where fr.is_yelp_recommended is true and mnl.label = 'Yelp - Austin') as avg_score_austin,
        avg(fr.score) filter (where fr.is_yelp_recommended is true and mnl.label = 'Yelp - Atlanta') as avg_score_atlanta,
        avg(fr.score) filter (where fr.is_yelp_recommended is true and mnl.label = '') as avg_score_no_locations,
        avg(fr.score) filter (where fr.is_yelp_recommended is true and mnl.label = 'Yelp - Baltimore') as avg_score_baltimore,
        avg(fr.score) filter (where fr.is_yelp_recommended is true and mnl.label = 'Yelp - Maryland DC') as avg_score_maryland_dc
    from feedback_review fr
    left join marketing_localaccount mnl on mnl.id = fr.account_id
    where mnl.channel_id = 1 and fr.deleted_at is null
    group by 1
  )
select
    cs.day as date,
    date_trunc('{period}',cs.day)::date as day,
    coalesce(sum(yd.positive_reviews) over (order by cs.day),0) as MAR188,
    coalesce(sum(yd.all_reviews) over (order by cs.day),0) as all_reviews,
    coalesce(sum(yd.positive_reviews_palo_alto) over (order by cs.day),0) as MAR284,
    coalesce(sum(yd.positive_reviews_houston) over (order by cs.day),0) as MAR1062,
    coalesce(sum(yd.positive_reviews_san_antonio) over (order by cs.day),0) as MAR1109,
    coalesce(sum(yd.positive_reviews_austin) over (order by cs.day),0) as MAR1103,
    coalesce(sum(yd.positive_reviews_atlanta) over (order by cs.day),0) as MAR1063,
        coalesce(sum(yd.positive_reviews_san_jose) over (order by cs.day), 0) as MAR290,
          coalesce(sum(yd.positive_reviews_oakland) over (order by cs.day), 0) as MAR288,
          coalesce(sum(yd.positive_reviews_san_francisco) over (order by cs.day), 0) as MAR343,
    coalesce(sum(yd.positive_reviews_sacramento) over (order by cs.day), 0) as MAR286,
        coalesce(sum(yd.positive_reviews_napa) over (order by cs.day), 0) as MAR291,
    coalesce(sum(yd.positive_reviews_watsonville) over (order by cs.day), 0) as MAR285,
        coalesce(sum(yd.positive_reviews_fresno) over (order by cs.day), 0) as MAR289,
        coalesce(sum(yd.positive_reviews_stockton) over (order by cs.day), 0) as MAR287,
        coalesce(sum(yd.positive_reviews_riverside) over (order by cs.day),0) as MAR574,
        coalesce(sum(yd.positive_reviews_lake_forest) over (order by cs.day),0) as MAR575,
        coalesce(sum(yd.positive_reviews_thousand_oaks) over (order by cs.day),0) as MAR576,
         coalesce(sum(yd.positive_reviews_los_angeles) over (order by cs.day),0) as MAR713,
         coalesce(sum(yd.positive_reviews_san_diego) over (order by cs.day),0) as MAR934,
        coalesce(sum(yd.positive_reviews_dallas) over (order by cs.day),0) as MAR935,
         coalesce(sum(yd.positive_reviews_fort_worth) over (order by cs.day),0) as MAR936,
        coalesce(sum(yd.all_reviews_palo_alto) over (order by cs.day),0) as all_reviews_paloalto,
        coalesce(sum(yd.all_reviews_san_jose) over (order by cs.day), 0) as all_reviews_san_jose,
        coalesce(sum(yd.all_reviews_oakland) over (order by cs.day), 0) as all_reviews_oakland,
        coalesce(sum(yd.all_reviews_san_francisco) over (order by cs.day), 0) as all_reviews_san_francisco,
    coalesce(sum(yd.all_reviews_sacramento) over (order by cs.day), 0) as all_reviews_sacramento,
        coalesce(sum(yd.all_reviews_napa) over (order by cs.day), 0) as all_reviews_napa,
    coalesce(sum(yd.all_reviews_watsonville) over (order by cs.day), 0) as all_reviews_watsonville,
         coalesce(sum(yd.all_reviews_fresno) over (order by cs.day), 0) as all_reviews_fresno,
    coalesce(sum(yd.all_reviews_stockton) over (order by cs.day), 0) as all_reviews_stockton,
    coalesce(sum(yd.all_reviews_riverside) over (order by cs.day),0) as all_reviews_riverside,
    coalesce(sum(yd.all_reviews_lake_forest) over (order by cs.day),0) as all_reviews_lake_forest,
    coalesce(sum(yd.all_reviews_thousand_oaks) over (order by cs.day),0) as all_reviews_thousand_oaks,
    coalesce(sum(yd.all_reviews_los_angeles) over (order by cs.day),0) as all_reviews_los_angeles,
    coalesce(sum(yd.all_reviews_san_diego) over (order by cs.day),0) as all_reviews_san_diego,
    coalesce(sum(yd.all_reviews_dallas) over (order by cs.day),0) as all_reviews_dallas,
    coalesce(sum(yd.all_reviews_fort_worth) over (order by cs.day),0) as all_reviews_fort_worth,
    coalesce(sum(yd.all_reviews_houston) over (order by cs.day),0) as all_reviews_houston,
    coalesce(sum(yd.all_reviews_san_antonio) over (order by cs.day),0) as all_reviews_san_antonio,
    coalesce(sum(yd.all_reviews_austin) over (order by cs.day),0) as all_reviews_austin,
    coalesce(sum(yd.all_reviews_atlanta) over (order by cs.day),0) as all_reviews_atlanta,
    coalesce(sum(yd.all_reviews_baltimore) over (order by cs.day), 0) as all_reviews_baltimore,
    coalesce(sum(yd.all_reviews_maryland_dc) over (order by cs.day), 0) as all_reviews_maryland_dc,
    coalesce(avg(yd.avg_score) over (order by cs.day),0) as MAR274,
    coalesce(avg(yd.avg_score_palo_alto) over (order by cs.day), 0) as MAR275,
    coalesce(avg(yd.avg_score_san_jose) over (order by cs.day), 0) as MAR281,
    coalesce(avg(yd.avg_score_oakland) over (order by cs.day), 0) as MAR279,
    coalesce(avg(yd.avg_score_san_francisco) over (order by cs.day), 0) as MAR342,
    coalesce(avg(yd.avg_score_sacramento) over (order by cs.day), 0) as MAR277,
    coalesce(avg(yd.avg_score_napa) over (order by cs.day), 0) as MAR282,
    coalesce(avg(yd.avg_score_watsonville) over (order by cs.day), 0) as MAR276,
    coalesce(avg(yd.avg_score_fresno) over (order by cs.day), 0) as MAR280,
    coalesce(avg(yd.avg_score_stockton) over (order by cs.day), 0) as MAR278,
    coalesce(avg(yd.avg_score_riverside) over (order by cs.day), 0) as MAR580,
    coalesce(avg(yd.avg_score_lake_forest) over (order by cs.day),0) as MAR581,
    coalesce(avg(yd.avg_score_thousand_oaks) over (order by cs.day),0) as MAR582,
    coalesce(avg(yd.avg_score_los_angeles) over (order by cs.day),0) as MAR712,
    coalesce(avg(yd.avg_score_san_diego) over (order by cs.day),0) as MAR928,
    coalesce(avg(yd.avg_score_dallas) over (order by cs.day),0) as MAR929,
    coalesce(avg(yd.avg_score_fort_worth) over (order by cs.day),0) as MAR930,
    coalesce(avg(yd.avg_score_houston) over (order by cs.day),0) as MAR1027,
    coalesce(avg(yd.avg_score_san_antonio) over (order by cs.day),0) as MAR1107,
    coalesce(avg(yd.avg_score_austin) over (order by cs.day),0) as MAR1108,
    coalesce(avg(yd.avg_score_atlanta) over (order by cs.day),0) as MAR1061,
    coalesce(avg(yd.avg_score_baltimore) over (order by cs.day), 0) as MAR1116,
    coalesce(avg(yd.avg_score_maryland_dc) over (order by cs.day), 0) as MAR1157,
    coalesce(sum(yd.positive_reviews_baltimore) over (order by cs.day), 0) as MAR1117,
    coalesce(sum(yd.positive_reviews_maryland_dc) over (order by cs.day), 0) as MAR1158
from calc_series cs
left join yelp_data yd on cs.day = yd.date_id
order by 1 desc
) as full_data
order by 1 desc
) as complete_data
where rank = '1'
