CREATE OR REPLACE VIEW VW_SM_TARGET_PORTAL_LEAK_S3
AS 
		SELECT 
			I.IPS_ACCOUNTNUMBER
            ,s.M_UNIT_ID
          ,case when lastReadingDateBelowThreshold.M_METER_TIME is NULL then '' else to_varchar(lastReadingDateBelowThreshold.M_METER_TIME, 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"') end as lastReadingDateBelowThreshold
          , to_varchar(lastReadingDate.M_METER_TIME, 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"') as lastReadingDate
          ,current_timestamp() as etl_load_datetime
        FROM 
         ( SELECT DISTINCT
			  M_UNIT_ID
			 FROM 
			 DATAHUB_TARGET.SM_APSY_SMINF_CUR
			 WHERE M_RESOURCE_TYPE ='Water_Flow_Readings_30min'
			 ) S JOIN DATAHUB_TARGET.VW_IPS_SM_ACCOUNT_UNITID I ON I.IPS_UNITID=S.M_UNIT_ID
       INNER JOIN (select distinct SMARTMETERID from DATAHUB_EXTRACT.SM_APSY_SMINF_PILOT2) PILOT ON PILOT.SMARTMETERID=S.M_UNIT_ID   
             INNER JOIN
             (
               SELECT DISTINCT  
			  M_UNIT_ID
              ,max(dateadd('m',-30,dateadd('ms',M_METER_TIME/1000,'1970-01-01'))) as M_METER_TIME
               
			 FROM 
			 DATAHUB_TARGET.SM_APSY_SMINF_CUR
			 WHERE M_RESOURCE_TYPE ='Water_Flow_Readings_30min'
             group by
             M_UNIT_ID             
               )lastReadingDate on lastReadingDate.M_UNIT_ID=s.M_UNIT_ID
             LEFT JOIN
             (
                 select
                 max(M_METER_TIME) as M_METER_TIME
                 ,M_UNIT_ID
                 from
               (
                 SELECT DISTINCT  
                M_UNIT_ID
                ,dateadd('m',-30,dateadd('ms',M_METER_TIME/1000,'1970-01-01')) as M_METER_TIME 
                 ,M_VALUE
               FROM 
               DATAHUB_TARGET.SM_APSY_SMINF_CUR
               WHERE M_RESOURCE_TYPE ='Water_Flow_Readings_30min' 
                 ) lowt 
                 JOIN DATAHUB_TARGET.VW_IPS_SM_ACCOUNT_UNITID isl ON isl.IPS_UNITID=lowt.M_UNIT_ID 
                 where
                 lowt.M_VALUE*isl.IPS_CONSUMPTIONPERCENTAGE<2.5
                 group by
                 M_UNIT_ID          
               )lastReadingDateBelowThreshold                            
               on lastReadingDateBelowThreshold.M_UNIT_ID=s.M_UNIT_ID

  
  ;
  