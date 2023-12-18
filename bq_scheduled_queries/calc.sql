with calc_data as (
select
  v.id,
  t.item as type,
  a.name as attr_name,
  a.var_name as attr_varname ,
  extract(date from a.created_at at time zone 'America/Los_Angeles') as attr_created_at, 
  v.name as value_name,
  v.code as value_code,
  v.amount as value_amount,
  v.sequence as value_sequence,
  extract(date from v.created_at at time zone 'America/Los_Angeles') as value_created_at,
  v.desc  as value_desc,
  extract(date from a.deleted_at at time zone 'America/Los_Angeles') as attr_deleted_at,
  extract(date from v.deleted_at at time zone 'America/Los_Angeles') as value_deleted_at,
  concat("https://api.ergeon.in/public-admin/calc/attr/",a.id,"/change/") as attr_link,
  concat("https://api.ergeon.in/public-admin/calc/value/",v.id,"/change/") as value_link,
  concat('https://api-ergeon-in.s3.amazonaws.com/calc_schematics/thumbnails/',a.var_name,'/',v.code,'.png') as thumbnail_image_png,
  concat('https://api-ergeon-in.s3.amazonaws.com/calc_schematics/thumbnails/',a.var_name,'/',v.code,'.jpg') as thumbnail_image_jpg,
  concat('https://api-ergeon-in.s3.amazonaws.com/calc_schematics/thumbnails/',a.var_name,'/',v.code,'.svg') as image_svg
from
  ergeon.product_catalogtype t join 
  ergeon.calc_configvalue cv on cv.config_id = t.config_id join
  ergeon.calc_value cvv on cvv.id = cv.value_id join
  ergeon.calc_attr a on a.id = cvv.attr_id join
  ergeon.calc_value v on v.attr_id = a.id 
order by
  t.item,
  a.name,
  v.sequence
)
select * from calc_data where attr_created_at is not null