CREATE OR REPLACE TABLE TARGET_HISTORY_LN.LN_TFFAM500_DELETED(
            compnr integer, 
            deleted boolean, 
            desc object, 
            dflt integer, 
            dflt_kw varchar, 
            locd varchar, 
            lock integer, 
            losk integer, 
            losk_ref_compnr integer, 
            sequencenumber integer, 
            timestamp datetime, 
            username varchar, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 