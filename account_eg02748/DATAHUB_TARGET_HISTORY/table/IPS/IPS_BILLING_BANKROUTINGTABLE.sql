CREATE OR REPLACE TABLE DATAHUB_TARGET_HISTORY.IPS_DELETED_BILLING_BANKROUTINGTABLE(
            ADDBY varchar, 
            ADDDTTM datetime, 
            BNKRTTBLKEY integer, 
            DDBANK varchar, 
            DDBRANCH varchar, 
            DDROUTNO varchar, 
            DELETED boolean, 
            MODBY varchar, 
            MODDTTM datetime, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 