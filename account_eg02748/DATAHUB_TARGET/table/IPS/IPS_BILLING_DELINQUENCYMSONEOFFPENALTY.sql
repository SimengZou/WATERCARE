CREATE OR REPLACE TABLE DATAHUB_TARGET.IPS_BILLING_DELINQUENCYMSONEOFFPENALTY(
            ADDBY varchar, 
            ADDDTTM datetime, 
            CONDITIONFORMULA integer, 
            DELETED boolean, 
            DELINQUENCYMSONEOFFPENKEY integer, 
            DISPLAYORDER integer, 
            MILESTONEKEY integer, 
            MODBY varchar, 
            MODDTTM datetime, 
            ONEOFFCHARGEKEY integer, 
            PERFORMON varchar, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 