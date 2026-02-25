CREATE OR REPLACE TABLE DATAHUB_TARGET_HISTORY.IPS_DELETED_BILLING_APPROVALLEVELROLE(
            ADDBY varchar, 
            ADDDTTM datetime, 
            APPROVALLEVELKEY integer, 
            APPROVALLEVELROLEKEY integer, 
            APPROVALROLEKEY integer, 
            DELETED boolean, 
            MODBY varchar, 
            MODDTTM datetime, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 