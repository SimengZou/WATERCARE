CREATE OR REPLACE TABLE DATAHUB_TARGET.IPS_BILLING_ACCOUNTTYPESETUPDETAIL(
            ACCOUNTTYPESETUPDETAILKEY integer, 
            ADDBY varchar, 
            ADDDTTM datetime, 
            CLASSNAME varchar, 
            DATALAKE_DELETED boolean, 
            DESCRIPTION varchar, 
            MODBY varchar, 
            MODDTTM datetime, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 