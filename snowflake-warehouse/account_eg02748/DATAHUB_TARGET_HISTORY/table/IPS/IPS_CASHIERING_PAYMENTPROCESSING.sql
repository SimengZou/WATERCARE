CREATE OR REPLACE TABLE DATAHUB_TARGET_HISTORY.IPS_DELETED_CASHIERING_PAYMENTPROCESSING(
            ADDBY varchar, 
            ADDDTTM datetime, 
            DATALAKE_DELETED boolean, 
            DESCRIPTION varchar, 
            EFFECTIVEDATE datetime, 
            EXPIREDATE datetime, 
            MODBY varchar, 
            MODDTTM datetime, 
            NAME varchar, 
            PAYMENTPROCESSINGKEY integer, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 