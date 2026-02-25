CREATE OR REPLACE TABLE TARGET_LN.LN_TCMCS145(
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
            etl_load_metadatafilename varchar
            ); 