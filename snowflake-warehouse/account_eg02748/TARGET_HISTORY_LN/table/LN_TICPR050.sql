CREATE OR REPLACE TABLE TARGET_HISTORY_LN.LN_TICPR050_DELETED(
            ccur varchar, 
            ccur_ref_compnr integer, 
            compnr integer, 
            deleted boolean, 
            dsca object, 
            oprc varchar, 
            sequencenumber integer, 
            timestamp datetime, 
            username varchar, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 