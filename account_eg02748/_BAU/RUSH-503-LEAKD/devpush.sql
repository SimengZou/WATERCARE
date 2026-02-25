copy into s3://wsl-dev-deinf-smapp-leakdetection-s3/
			FROM (SELECT  OBJECT_CONSTRUCT('accountNumber', IPS_ACCOUNTNUMBER, 'meterId', M_UNIT_ID, 'lastReadingDateBelowThreshold', lastReadingDateBelowThreshold,'lastReadingDate',lastReadingDate)   FROM (
			
		        select 
                    IPS_ACCOUNTNUMBER
                    ,M_UNIT_ID
                    ,lastReadingDateBelowThreshold
                    ,lastReadingDate
                from 
                    DATAHUB_INTEGRATION.VW_SM_STREAM_PORTAL_LEAK_S3
		
			 )
			 
			 )
			storage_integration = DEV_WSL_SMAPP_LEAKDETECTION_S3
			FILE_FORMAT = (TYPE = JSON COMPRESSION = NONE)
			OVERWRITE = TRUE
			encryption=(type='AWS_SSE_S3')
			MAX_FILE_SIZE=9999;