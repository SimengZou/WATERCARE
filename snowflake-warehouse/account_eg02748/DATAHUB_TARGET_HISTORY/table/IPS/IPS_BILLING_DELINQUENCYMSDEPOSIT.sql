CREATE OR REPLACE TABLE DATAHUB_TARGET_HISTORY.IPS_DELETED_BILLING_DELINQUENCYMSDEPOSIT(
            ADDBY varchar, 
            ADDDTTM datetime, 
            CONDITIONFORMULA integer, 
            DELETED boolean, 
            DELINQUENCYMSDEPOSITKEY integer, 
            DEPOSITCHARGEKEY integer, 
            DISPLAYORDER integer, 
            MILESTONEKEY integer, 
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