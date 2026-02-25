CREATE OR REPLACE TABLE DATAHUB_TARGET.IPS_BILLING_BILLTYPELINEITEMSETUP(
            ADDBY varchar, 
            ADDDTTM datetime, 
            BILLTYPEKEY integer, 
            BILLTYPELINEITEMSETUPKEY integer, 
            CALCULATIONORDER integer, 
            DELETED boolean, 
            LINEITEMSETUPKEY integer, 
            MODBY varchar, 
            MODDTTM datetime, 
            PENALTYPAYORDER integer, 
            PRINCIPALPAYORDER integer, 
            PRINTORDER integer, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 