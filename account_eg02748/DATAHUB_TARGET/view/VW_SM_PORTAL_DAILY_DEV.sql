create or replace view VW_SM_PORTAL_DAILY_DEV(
	IPS_ACCOUNTNUMBER,
	M_METER_TIME,
	M_VALUE,
	IPS_CONSUMPTIONPERCENTAGE,
	MISSINGINTERVALCOUNT,
	M_METER_TIME_DATE
) as
			select 
IPS_ACCOUNTNUMBER
,M_METER_TIME
,SUm(M_VALUE) as  M_VALUE
,IPS_CONSUMPTIONPERCENTAGE
,cast(48-coalesce(differentialreadingcount,0) as text) as missingIntervalCount
,M_METER_TIME_date
from
(
            SELECT 
			i.IPS_ACCOUNTNUMBER
			,concat(to_varchar(dateadd('m',-30,dateadd('ms',s.M_METER_TIME/1000,'1970-01-01')), 'YYYY-MM-DD') ,'#',s.M_UNIT_ID) AS M_METER_TIME         
			,M_VALUE*IPS_CONSUMPTIONPERCENTAGE AS M_VALUE
			,IPS_CONSUMPTIONPERCENTAGE
            ,to_varchar(dateadd('m',-30,dateadd('ms',s.M_METER_TIME/1000,'1970-01-01')), 'YYYY-MM-DD') as M_METER_TIME_date
            ,count(M_VALUE) over (partition by i.IPS_ACCOUNTNUMBER,to_varchar(dateadd('m',-30,dateadd('ms',s.M_METER_TIME/1000,'1970-01-01')), 'YYYY-MM-DD'),s.M_UNIT_ID) as differentialreadingcount
            FROM 
			( select distinct
			  M_UNIT_ID
			  ,M_METER_TIME
			  ,M_VALUE
             
             FROM 
			 DATAHUB_target.SM_APSY_SMINF_CUR
			 WHERE M_RESOURCE_TYPE ='Water_Flow_Readings_30min'
			 ) s JOIN DATAHUB_TARGET.VW_IPS_SM_ACCOUNT_UNITID i ON i.IPS_UNITID=s.M_UNIT_ID   
			 INNER JOIN (select distinct
			  s.M_UNIT_ID
             FROM 
			 DATAHUB_target.SM_APSY_SMINF_CUR s
		     JOIN DATAHUB_TARGET.VW_IPS_SM_ACCOUNT_UNITID i ON i.IPS_UNITID=s.M_UNIT_ID  AND M_RESOURCE_TYPE ='Water_Flow_Readings_30min' 
			 INNER JOIN (SELECT DISTINCT SMARTMETERID FROM DATAHUB_LANDING.SM_APSY_SMINF_PILOT) pilot ON pilot.SMARTMETERID=s.M_UNIT_ID ) pilot ON pilot.M_UNIT_ID=s.M_UNIT_ID
) 
group by
IPS_ACCOUNTNUMBER
,M_METER_TIME
,IPS_CONSUMPTIONPERCENTAGE
,differentialreadingcount
,M_METER_TIME_date;