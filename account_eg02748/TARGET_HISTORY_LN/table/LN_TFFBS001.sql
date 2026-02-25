CREATE OR REPLACE TABLE TARGET_HISTORY_LN.LN_TFFBS001_DELETED(
            compnr integer, 
            deleted boolean, 
            desc object, 
            disb varchar, 
            dism integer, 
            dism_kw varchar, 
            ndpr integer, 
            sequencenumber integer, 
            timestamp datetime, 
            username varchar, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 