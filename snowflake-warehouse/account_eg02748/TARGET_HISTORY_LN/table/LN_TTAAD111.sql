CREATE OR REPLACE TABLE TARGET_HISTORY_LN.LN_TTAAD111_DELETED(
            compnr integer, 
            deleted boolean, 
            dlan varchar, 
            icnt varchar, 
            icnt_ref_compnr integer, 
            sequencenumber integer, 
            stat integer, 
            stat_kw varchar, 
            timestamp datetime, 
            username varchar, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 