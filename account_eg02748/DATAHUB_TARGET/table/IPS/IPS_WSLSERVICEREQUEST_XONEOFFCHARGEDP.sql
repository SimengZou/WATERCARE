CREATE OR REPLACE TABLE DATAHUB_TARGET.IPS_WSLSERVICEREQUEST_XONEOFFCHARGEDP(
            ADDBY varchar, 
            ADDDTTM datetime, 
            AMOUNT numeric(38, 10), 
            DELETED boolean, 
            MODBY varchar, 
            MODDTTM datetime, 
            ONEOFFCHARGE varchar, 
            SERVNO integer, 
            VARIATION_ID integer, 
            XONEOFFCHARGEDPKEY integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 