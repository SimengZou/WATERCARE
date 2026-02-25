CREATE OR REPLACE TABLE TARGET_LN.LN_TTAAD420(
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
            etl_load_metadatafilename varchar
            ); 