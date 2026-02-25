CREATE OR REPLACE TABLE TARGET_HISTORY_LN.LN_TCCCP070_DELETED(
            compnr integer, 
            cpdt varchar, 
            cpdt_ref_compnr integer, 
            deleted boolean, 
            dsca object, 
            endt datetime, 
            peri integer, 
            sequencenumber integer, 
            stdt datetime, 
            timestamp datetime, 
            username varchar, 
            yrno integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 