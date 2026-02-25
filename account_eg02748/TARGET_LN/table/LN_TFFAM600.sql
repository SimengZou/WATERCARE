CREATE OR REPLACE TABLE TARGET_LN.LN_TFFAM600(
            book varchar, 
            compnr integer, 
            deleted boolean, 
            depr integer, 
            depr_kw varchar, 
            desc object, 
            fdty integer, 
            fdty_kw varchar, 
            sequencenumber integer, 
            timestamp datetime, 
            type integer, 
            type_kw varchar, 
            username varchar, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 