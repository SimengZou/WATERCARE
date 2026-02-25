CREATE OR REPLACE TABLE TARGET_HISTORY_LN.LN_TXPDM908_DELETED(
            compnr integer, 
            cprj varchar, 
            cprj_ref_compnr integer, 
            deleted boolean, 
            pmre varchar, 
            pmre_ref_compnr integer, 
            sequencenumber integer, 
            timestamp datetime, 
            username varchar, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 