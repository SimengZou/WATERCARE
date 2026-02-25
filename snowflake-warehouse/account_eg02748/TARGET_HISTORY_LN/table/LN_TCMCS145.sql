CREATE OR REPLACE TABLE TARGET_HISTORY_LN.LN_TCMCS145_DELETED(
            compnr integer, 
            deleted boolean, 
            dsca object, 
            rgrp varchar, 
            sequencenumber integer, 
            timestamp datetime, 
            username varchar, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 