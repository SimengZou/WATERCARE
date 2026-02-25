CREATE OR REPLACE TABLE TARGET_LN.LN_TPPDM055(
            compnr integer, 
            csec varchar, 
            deleted boolean, 
            desc object, 
            sequencenumber integer, 
            timestamp datetime, 
            username varchar, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 