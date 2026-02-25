CREATE OR REPLACE TABLE DATAHUB_TARGET.IPS_BILLING_OOCCUSTPROB(
            ADDBY varchar, 
            ADDDTTM datetime, 
            DELETED boolean, 
            MODBY varchar, 
            MODDTTM datetime, 
            ONEOFFCHARGEKEY integer, 
            OOCCUSTPROBKEY integer, 
            SERVNO integer, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 