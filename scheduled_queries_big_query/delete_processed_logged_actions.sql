DELETE FROM `bigquerydatabase-270315.ergeon_replication.logged_actions_update`
where id in
(
  with
    max_processed_id_per_table as
  (
    select
      table,
      max(max_processed_id) as max_processed_id
    from ergeon_replication.merge_job_log
    where cast(created_at as date) >= current_date()
    group by 1
  )
  select
    id
  from ergeon_replication.logged_actions_update la
  left join max_processed_id_per_table mp on mp.table = la.table_name
  where la.id <= mp.max_processed_id and action <> 'D'
);