CREATE OR REPLACE TABLE DATAHUB_TARGET.IPS_CRM_CUSTLOG(
            ADDBY varchar, 
            ADDDTTM datetime, 
            COMMENTS varchar, 
            CUSTLOGKEY integer, 
            DATALAKE_DELETED boolean, 
            LOGBY varchar, 
            LOGFRDTTM datetime, 
            LOGTODTTM datetime, 
            LOGTYPE varchar, 
            MODBY varchar, 
            MODDTTM datetime, 
            SERVNO integer, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 