CREATE OR REPLACE TABLE DATAHUB_TARGET_HISTORY.IPS_DELETED_WSLINTERFACE_WSLCUSTOMERALERTS(
            ACCOUNTALERTTYPE varchar, 
            ACCOUNTNUMBER varchar, 
            ADDBY varchar, 
            ADDDTTM datetime, 
            ADDRESSKEY integer, 
            DATALAKE_DELETED boolean, 
            EFFECTIVEDATE datetime, 
            EXPIREDATE datetime, 
            MODBY varchar, 
            MODDTTM datetime, 
            VARIATION_ID integer, 
            WSLCUSTOMERALERTSKEY integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 