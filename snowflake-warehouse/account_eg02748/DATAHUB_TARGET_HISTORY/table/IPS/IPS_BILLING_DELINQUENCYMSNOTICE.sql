CREATE OR REPLACE TABLE DATAHUB_TARGET_HISTORY.IPS_DELETED_BILLING_DELINQUENCYMSNOTICE(
            ADDBY varchar, 
            ADDDTTM datetime, 
            CONDITIONFORMULA integer, 
            CORRPROCESSSETUPKEY integer, 
            DELETED boolean, 
            DISPLAYORDER integer, 
            MILESTONEKEY integer, 
            MILESTONENOTICEKEY integer, 
            MODBY varchar, 
            MODDTTM datetime, 
            PERFORMON varchar, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 