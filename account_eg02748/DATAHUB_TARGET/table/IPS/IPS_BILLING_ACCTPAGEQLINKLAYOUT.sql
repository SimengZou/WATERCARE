CREATE OR REPLACE TABLE DATAHUB_TARGET.IPS_BILLING_ACCTPAGEQLINKLAYOUT(
            ACCTPAGEQLINKLAYOUTKEY integer, 
            ACCTPAGEQLINKSUKEY integer, 
            ADDBY varchar, 
            ADDDTTM datetime, 
            DATALAKE_DELETED boolean, 
            DISPLAYFLAG varchar, 
            DISPLAYORDER integer, 
            MODBY varchar, 
            MODDTTM datetime, 
            QUICKLINK varchar, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 