CREATE OR REPLACE TABLE DATAHUB_TARGET_HISTORY.IPS_DELETED_CASHIERING_REGTRANESCROWPAYMENTLINK(
            ADDBY varchar, 
            ADDDTTM datetime, 
            DELETED boolean, 
            DRAWTRANNO integer, 
            ESCROWACCTKEY integer, 
            ESCROWPAYKEY integer, 
            MODBY varchar, 
            MODDTTM datetime, 
            REGTRANNO integer, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 