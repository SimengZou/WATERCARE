CREATE OR REPLACE TABLE DATAHUB_TARGET.IPS_BILLING_APPROVALLINK(
            ADDBY varchar, 
            ADDDTTM datetime, 
            ADJTYPESUKEY integer, 
            AMOUNT numeric(38, 10), 
            APPROVALSETUP integer, 
            COSIGN varchar, 
            DELETED boolean, 
            MODBY varchar, 
            MODDTTM datetime, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 