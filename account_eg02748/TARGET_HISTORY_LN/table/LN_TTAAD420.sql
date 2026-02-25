CREATE OR REPLACE TABLE TARGET_HISTORY_LN.LN_TTAAD420_DELETED(
            compnr integer, 
            deleted boolean, 
            fcmp integer, 
            fcmp_ref_compnr integer, 
            lcmp integer, 
            lcmp_ref_compnr integer, 
            sequencenumber integer, 
            tabg integer, 
            tabg_kw varchar, 
            tabl varchar, 
            timestamp datetime, 
            username varchar, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 