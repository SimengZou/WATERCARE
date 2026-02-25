CREATE OR REPLACE TABLE DATAHUB_TARGET.IPS_BILLING_DELINQUENCYGROUP(
            ADDBY varchar, 
            ADDDTTM datetime, 
            CODE varchar, 
            DELETED boolean, 
            DELINQUENCYSETUPKEY integer, 
            DISPLAYORDER integer, 
            ENTRYSCHEMEFORMULA integer, 
            GROUPKEY integer, 
            MINENTRYAMT numeric(38, 10), 
            MODBY varchar, 
            MODDTTM datetime, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 