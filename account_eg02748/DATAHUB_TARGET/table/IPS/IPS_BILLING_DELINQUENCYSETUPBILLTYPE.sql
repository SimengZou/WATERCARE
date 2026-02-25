CREATE OR REPLACE TABLE DATAHUB_TARGET.IPS_BILLING_DELINQUENCYSETUPBILLTYPE(
            ADDBY varchar, 
            ADDDTTM datetime, 
            BILLTYPEKEY integer, 
            DATALAKE_DELETED boolean, 
            DELINQUENCYSETUPKEY integer, 
            MODBY varchar, 
            MODDTTM datetime, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 