create or replace view VW_SM_LANDING_TARGET_COMPARISON_30MIN(
	M_UNIT_ID,
	M_METER_TIME_DATE,
	LANDING_SUM_M_VALUE,
	TARGET_SUM_M_VALUE
) as
select 
landing.m_unit_id
,landing.M_METER_TIME_DATE
,landing.sum_m_value as LANDING_sum_m_value
,target.sum_m_value as TARGET_sum_m_value
from 
(
select
sum(m_value) as sum_m_value
,cast(dateadd('ms',M_METER_TIME/1000,'1970-01-01') as date) as M_METER_TIME_DATE

,m_unit_id
    from
DATAHUB_LANDING.vw_SM_APSY_SMINF_CUR_landing
WHERE M_RESOURCE_TYPE ='Water_Flow_Readings_30min'
group by
cast(dateadd('ms',M_METER_TIME/1000,'1970-01-01') as date),m_unit_id ) landing
left join
(
select
sum(m_value) as sum_m_value
,cast(dateadd('ms',M_METER_TIME/1000,'1970-01-01') as date) as M_METER_TIME_DATE
,m_unit_id
    from
DATAHUB_TARGET.SM_APSY_SMINF_CUR
WHERE M_RESOURCE_TYPE ='Water_Flow_Readings_30min'
group by
cast(dateadd('ms',M_METER_TIME/1000,'1970-01-01') as date),m_unit_id ) target
on landing.m_unit_id=target.m_unit_id and landing.M_METER_TIME_DATE=target.M_METER_TIME_DATE
WHERE
target.sum_m_value IS NULL OR landing.sum_m_value<>target.sum_m_value
;