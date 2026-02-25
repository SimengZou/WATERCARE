create or replace view DATAHUB_EXTRACT.VW_SM_STREAM_CURATED_MEASUREMENT_LEAK(
	IPS_ACCOUNTNUMBER,
	M_UNIT_ID,
	LASTREADINGDATEBELOWTHRESHOLD,
	LASTREADINGDATE,
	ETL_LOAD_DATETIME
) as 
	SELECT 
		I.IPS_ACCOUNTNUMBER
		,s.M_UNIT_ID
		,case when lastReadingDateBelowThreshold.M_METER_TIME is NULL then '' else to_varchar(lastReadingDateBelowThreshold.M_METER_TIME, 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"') end as lastReadingDateBelowThreshold
		, to_varchar(lastReadingDate.M_METER_TIME, 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"') as lastReadingDate
		,current_timestamp() as etl_load_datetime
	FROM 
	(
		SELECT DISTINCT
			M_UNIT_ID
		FROM DATAHUB_EXTRACT.STREAM_SM_CURATED_MEASUREMENT_LEAK
		WHERE M_RESOURCE_TYPE ='Water_Flow_Readings_30min'
	) S 
	
	INNER JOIN (SELECT DISTINCT
                    a.accountnumber IPS_ACCOUNTNUMBER,
                    c.unitid IPS_UNITID
               		,1 AS IPS_CONSUMPTIONPERCENTAGE
                FROM DATAHUB_TARGET.IPS_WSLASSETWATER_XMETERACCOUNT x
                INNER JOIN DATAHUB_TARGET.IPS_billing_account a ON x.account=a.accountkey
                INNER JOIN  DATAHUB_TARGET.IPS_Assetmanagement_Water_compwmtr c ON x.COMPKEY = c.COMPKEY
                WHERE a.accountstatus = 'A' 
				AND c.servstat = 'OP'
                AND c.UNITTYPE = 'MTSRD07'
                AND x.SERVICETYPE = 'Water'
                AND x.SHAREPERC = 100) I ON I.IPS_UNITID=S.M_UNIT_ID   
		
	INNER JOIN (select distinct w.unitid AS UNIT_ID
				from DATAHUB_TARGET.IPS_billing_account a
					inner join DATAHUB_TARGET.IPS_billing_accountservice s  on a.accountkey=s.accountkey
					inner join DATAHUB_TARGET.IPS_billing_accountserviceposition p  on s.accountservicekey=p.accountservicekey
					inner join DATAHUB_TARGET.IPS_assetmanagement_assloc l on p.addrkey=l.addrkey and p.position=l.position and l.remdt is null
					inner join DATAHUB_TARGET.IPS_assetmanagement_water_compwmtr w on l.compkey=w.compkey
				WHERE a.accountstatus = 'A' and
					w.servstat = 'OP') PILOT ON PILOT.UNIT_ID=S.M_UNIT_ID  
		
	INNER JOIN
	(
		SELECT DISTINCT  
			M_UNIT_ID
			,max(dateadd('m',-30,M_METER_TIME)) as M_METER_TIME
		FROM TARGET_SM.SM_CURATED_MEASUREMENT
		WHERE M_RESOURCE_TYPE ='Water_Flow_Readings_30min'
		group by M_UNIT_ID             
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
				,dateadd('m',-30,M_METER_TIME) M_METER_TIME
				,M_VALUE
			FROM TARGET_SM.SM_CURATED_MEASUREMENT
			WHERE M_RESOURCE_TYPE ='Water_Flow_Readings_30min' 
		) lowt 
		
		JOIN DATAHUB_TARGET.VW_IPS_SM_ACCOUNT_UNITID isl ON isl.IPS_UNITID=lowt.M_UNIT_ID 
		where lowt.M_VALUE*isl.IPS_CONSUMPTIONPERCENTAGE<2.5
		group by M_UNIT_ID          
	)lastReadingDateBelowThreshold                            
		on lastReadingDateBelowThreshold.M_UNIT_ID=s.M_UNIT_ID;