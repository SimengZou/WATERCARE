create or replace view VW_SM_APSY_SMINF_CUR_LASTREAD(
	M_UNIT_ID,
	LAST_M_METER_TIME,
	DAYS_SINCE_LAST_READ
) as 
SELECT 
m_unit_id,
MAX(dateadd('ms',m_meter_time /1000,'1970-01-01')) as m_meter_time,
datediff(DAY, MAX(dateadd('ms',m_meter_time /1000,'1970-01-01')), CURRENT_DATE())-1 as DAYS_SINCE_LAST_READ
FROM datahub_target.sm_apsy_sminf_cur
WHERE M_RESOURCE_TYPE ='Water_Flow_Readings_30min' 
GROUP BY m_unit_id;