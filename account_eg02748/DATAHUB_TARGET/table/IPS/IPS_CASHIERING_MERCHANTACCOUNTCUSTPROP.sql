CREATE OR REPLACE TABLE DATAHUB_TARGET.IPS_CASHIERING_MERCHANTACCOUNTCUSTPROP(
            ADDBY varchar, 
            ADDDTTM datetime, 
            DATALAKE_DELETED boolean, 
            MERCHANTACCOUNT integer, 
            MERCHANTACCOUNTCUSTPROPKEY integer, 
            MODBY varchar, 
            MODDTTM datetime, 
            NAME varchar, 
            VALUE varchar, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 