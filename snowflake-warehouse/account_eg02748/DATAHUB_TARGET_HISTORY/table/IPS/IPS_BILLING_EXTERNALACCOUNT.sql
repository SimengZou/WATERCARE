CREATE OR REPLACE TABLE DATAHUB_TARGET_HISTORY.IPS_DELETED_BILLING_EXTERNALACCOUNT(
            ADDBY varchar, 
            ADDDTTM datetime, 
            COMMENTS varchar, 
            DATALAKE_DELETED boolean, 
            EXTERNALACCOUNTKEY integer, 
            EXTERNALACCOUNTNO varchar, 
            EXTERNALACCOUNTTYPE varchar, 
            HANSENACCOUNTKEY integer, 
            MODBY varchar, 
            MODDTTM datetime, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 