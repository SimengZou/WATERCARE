CREATE OR REPLACE TABLE DATAHUB_TARGET.IPS_CASHIERING_PAYSERVERTRANCUSTPROP(
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
            etl_load_metadatafilename varchar
            ); 