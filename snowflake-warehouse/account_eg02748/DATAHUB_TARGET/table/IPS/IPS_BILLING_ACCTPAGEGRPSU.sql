CREATE OR REPLACE TABLE DATAHUB_TARGET.IPS_BILLING_ACCTPAGEGRPSU(
            ACCTPAGEGRPSUKEY integer, 
            ADDBY varchar, 
            ADDDTTM datetime, 
            DATALAKE_DELETED boolean, 
            DEFAULTSUFLAG varchar, 
            MODBY varchar, 
            MODDTTM datetime, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 