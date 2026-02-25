CREATE OR REPLACE TABLE DATAHUB_TARGET_HISTORY.IPS_DELETED_BILLING_ACCTHISTUSAGEDETAIL(
            ACCOUNTSERVICE integer, 
            ACCTHISTUSAGEDETAILKEY integer, 
            ADDBY varchar, 
            ADDDTTM datetime, 
            DATALAKE_DELETED boolean, 
            LINEITEM integer, 
            MODBY varchar, 
            MODDTTM datetime, 
            USAGE integer, 
            USAGEDATE datetime, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 