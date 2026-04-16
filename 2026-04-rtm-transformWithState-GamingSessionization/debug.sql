--get Counts of input stream by minute
with sessions_stream as (
  select appSessionId, eventId, hostPcId as deviceId, psnAccountId, timestamp, totalFgTime from gaming_sessionization_demo.rtm_workload.pc_sessions_stream
  union all 
  select appSessionId, eventId, openPsid as deviceId, psnAccountId, timestamp, totalFgTime from gaming_sessionization_demo.rtm_workload.console_sessions_stream
)
select date_trunc('MINUTE', `timestamp`), count(1)
from sessions_stream
group by date_trunc('MINUTE', `timestamp`)
order by date_trunc('MINUTE', `timestamp`)

--SessionStart vs SessionHeartbeat ratio from output 
select  
date_trunc('MINUTE', `session_timestamp`), 
sum(case when sessionStatus = 'SessionStart' then 1 else null end) as SessionStart,
sum(case when sessionStatus = 'SessionHeartbeat' then 1 else null end) as SessionHeartbeat,
sum(case when sessionStatus = 'SessionEnd' then 1 else null end) as SessionEnd
from gaming_sessionization_demo.rtm_workload.session_results_rtm
group by date_trunc('MINUTE', `session_timestamp`)
order by date_trunc('MINUTE', `session_timestamp`)


-- Debugging Timer heartbeats for the session scope - get some example deviceIds 
with sessions_stream as (
  select appSessionId, eventId, hostPcId as deviceId, psnAccountId, timestamp, totalFgTime from gaming_sessionization_demo.rtm_workload.pc_sessions_stream
  union all 
  select appSessionId, eventId, openPsid as deviceId, psnAccountId, timestamp, totalFgTime from gaming_sessionization_demo.rtm_workload.console_sessions_stream
),
minute_1 as (select deviceid, appSessionId, totalFgTime, `timestamp` as session_start from sessions_stream  where minute(`timestamp`) = 1 ),
minute_5 as (select deviceid, appSessionId, totalFgTime, `timestamp` as session_end from sessions_stream  where minute(`timestamp`) = 6 )

select * from minute_1 join minute_5 on minute_1.deviceid = minute_5.deviceid and minute_1.appSessionId = minute_5.appSessionId order by session_start, session_end 

-- Debugging Timer heartbeats for the session scope - pick a device id and 
select  * except(deviceId, psnAccountId)
from gaming_sessionization_demo.rtm_workload.session_results_rtm where deviceId = 'REPLACE_WITH_DEVICE_ID'
order by output_timestamp asc

-- Getting latency metrics 
with tab1 as (
  select deviceId, sessionStatus, appSessionId, psnAccountId, processing_timestamp, upstream_timestamp, output_timestamp, timestampdiff(MILLISECOND, upstream_timestamp, output_timestamp) as latency_ms
from  gaming_sessionization_demo.rtm_workload.session_results_rtm where sessionStatus in ('SessionStart', 'SessionEnd') 
order by timestampdiff(SECOND, upstream_timestamp, output_timestamp) asc
)
  select 
  count(1),
  min(latency_ms) as min,
  percentile(latency_ms, 0.10) as p10,
  percentile(latency_ms, 0.25) as p25,
  percentile(latency_ms, 0.5) as median,
  percentile(latency_ms, 0.75) as p75,
  percentile(latency_ms, 0.90) as p90,
  percentile(latency_ms, 0.95) as p95,
  percentile(latency_ms, 0.99) as p99,
  max(latency_ms) as max
  from tab1


