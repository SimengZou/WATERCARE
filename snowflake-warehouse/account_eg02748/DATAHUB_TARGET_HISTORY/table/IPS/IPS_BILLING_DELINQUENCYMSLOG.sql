CREATE OR REPLACE TABLE DATAHUB_TARGET_HISTORY.IPS_DELETED_BILLING_DELINQUENCYMSLOG(
            ADDBY varchar, 
            ADDDTTM datetime, 
            DELETED boolean, 
            DISPLAYORDER integer, 
            LOGTYPE varchar, 
            MILESTONEKEY integer, 
            MILESTONELOGKEY integer, 
            MODBY varchar, 
            MODDTTM datetime, 
            PERFORMON varchar, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 