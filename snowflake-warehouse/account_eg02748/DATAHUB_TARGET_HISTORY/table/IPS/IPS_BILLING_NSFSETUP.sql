CREATE OR REPLACE TABLE DATAHUB_TARGET_HISTORY.IPS_DELETED_BILLING_NSFSETUP(
            ADDBY varchar, 
            ADDDTTM datetime, 
            BILLTYPEKEY integer, 
            BILLTYPESELECTIONRULE integer, 
            CORRPROCESSSETUP integer, 
            DELETED boolean, 
            MODBY varchar, 
            MODDTTM datetime, 
            NSFSETUPKEY integer, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 