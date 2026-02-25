CREATE OR REPLACE TABLE DATAHUB_TARGET_HISTORY.IPS_DELETED_CASHIERING_PAYSERVERTRANCUSTPROP(
            ADDBY varchar, 
            ADDDTTM datetime, 
            DELETED boolean, 
            DISPLAY varchar, 
            MODBY varchar, 
            MODDTTM datetime, 
            PAYMENTSERVERTRANSACTIONKEY integer, 
            PAYSERVERTRANCUSTPROPKEY integer, 
            PROPNAME varchar, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 