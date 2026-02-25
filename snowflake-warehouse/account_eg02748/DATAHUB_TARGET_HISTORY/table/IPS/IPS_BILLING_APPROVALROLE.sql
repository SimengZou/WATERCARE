CREATE OR REPLACE TABLE DATAHUB_TARGET_HISTORY.IPS_DELETED_BILLING_APPROVALROLE(
            ADDBY varchar, 
            ADDDTTM datetime, 
            APPROVALROLEKEY integer, 
            APPROVALROLENAME varchar, 
            CATEGORY integer, 
            DELETED boolean, 
            DESCRIPTION varchar, 
            MODBY varchar, 
            MODDTTM datetime, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 