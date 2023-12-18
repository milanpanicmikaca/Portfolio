-- upload to BQ
select
  date_trunc(date, {period}) as date,
  count(*) as MAR181 -- blogpost_cnt
from googlesheets.blog_post bp
group by 1
order by 1 desc
