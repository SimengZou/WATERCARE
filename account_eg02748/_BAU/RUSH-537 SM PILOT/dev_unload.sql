copy into s3://wsl-apsy-de-sminf-portal-differential-s3/
			FROM (SELECT  OBJECT_CONSTRUCT('accountId', IPS_ACCOUNTNUMBER, 'readingTime', M_METER_TIME, 'readingValue', M_VALUE,'consumptionPercentage',IPS_CONSUMPTIONPERCENTAGE)   FROM (
			
		        select IPS_ACCOUNTNUMBER,M_METER_TIME,M_VALUE,IPS_CONSUMPTIONPERCENTAGE from DATAHUB_TARGET.VW_SM_TARGET_PORTAL_DIFFERENTIAL_S3
		
			 )
			 
			 )
			storage_integration = dev_sm_portal_differential
			FILE_FORMAT = (TYPE = JSON COMPRESSION = NONE)
			OVERWRITE = TRUE
			encryption=(type='AWS_SSE_S3')
			MAX_FILE_SIZE=9999;
