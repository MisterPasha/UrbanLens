with fixture as (
 select * from (values
 ('old_but_not_due','2026-11-04 08:00Z'::timestamptz,'accepted','2026-11-04 07:00Z'::timestamptz,
  '2026-12-07 08:00Z'::timestamptz,'2026-11-08 12:00Z'::timestamptz,'healthy'),
 ('grace_boundary','2026-11-04 08:00Z','accepted','2026-11-04 07:00Z','2026-12-07 08:00Z','2026-12-09 08:00Z','healthy'),
 ('overdue','2026-11-04 08:00Z','accepted','2026-11-04 07:00Z','2026-12-07 08:00Z','2026-12-09 08:00:01Z','stale'),
 ('unavailable',null,null,null,null,'2026-11-08 12:00Z','unavailable'),
 ('failure_retained','2026-11-04 08:00Z','failed','2026-11-05 07:00Z','2026-12-07 08:00Z','2026-11-08 12:00Z','failed'),
 ('rejected_retained','2026-11-04 08:00Z','rejected','2026-11-05 07:00Z','2026-12-07 08:00Z','2026-11-08 12:00Z','held')
 ) v(label,success,attempt_status,attempt_time,due,now,expected)
), computed as (
 select *,{{ health_state('success','attempt_status','attempt_time','due','now') }} actual from fixture
)
select label as failure from computed where actual is distinct from expected
union all select 'monthly_schedule'
where {{ next_monthly_due("'2026-11-04 08:00Z'::timestamptz",8) }} <> '2026-12-07 08:00Z'::timestamptz
union all select 'baseline_zero_unavailable_distinction' where exists(
 select 1 from {{ ref('mart_activity_status') }}
 where (state='baseline_unavailable' and retained_event_count is not null)
    or (state='no_observed_changes' and (established_at is null or retained_event_count<>0)))
