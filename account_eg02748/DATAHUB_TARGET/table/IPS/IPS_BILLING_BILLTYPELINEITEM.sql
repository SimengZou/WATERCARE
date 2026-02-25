CREATE OR REPLACE TABLE DATAHUB_TARGET.IPS_BILLING_BILLTYPELINEITEM(
            ADDBY varchar, 
            ADDDTTM datetime, 
            BILLTYPELINEITEMKEY integer, 
            CALCULATIONORDER integer, 
            CHILDKEY integer, 
            DELETED boolean, 
            MODBY varchar, 
            MODDTTM datetime, 
            PARENTKEY integer, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 