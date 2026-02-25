CREATE OR REPLACE TABLE TARGET_HISTORY_LN.LN_TFGLD006_DELETED(
            compnr integer, 
            deleted boolean, 
            fpdt date, 
            rpdt date, 
            sequencenumber integer, 
            timestamp datetime, 
            username varchar, 
            vpdt date, 
            ydsc object, 
            year integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 