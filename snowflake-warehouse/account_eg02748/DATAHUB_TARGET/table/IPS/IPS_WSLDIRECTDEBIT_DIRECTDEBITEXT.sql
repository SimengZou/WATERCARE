CREATE OR REPLACE TABLE DATAHUB_TARGET.IPS_WSLDIRECTDEBIT_DIRECTDEBITEXT(
            ADDBY varchar, 
            ADDDTTM datetime, 
            DELETED boolean, 
            DIRECTDEBITEXTKEY integer, 
            DIRECTDEBITRUNKEY integer, 
            ISPROCESSED varchar, 
            MODBY varchar, 
            MODDTTM datetime, 
            SCHEDULEHISTORYKEY integer, 
            SCHEDULEKEY integer, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 