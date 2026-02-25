CREATE OR REPLACE TABLE TARGET_HISTORY_LN.LN_TDSMI003_DELETED(
            compnr integer, 
            cpha varchar, 
            deleted boolean, 
            dsca object, 
            sequencenumber integer, 
            timestamp datetime, 
            username varchar, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 