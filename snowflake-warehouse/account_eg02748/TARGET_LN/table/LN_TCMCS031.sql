CREATE OR REPLACE TABLE TARGET_LN.LN_TCMCS031(
            cbrn varchar, 
            compnr integer, 
            deleted boolean, 
            dsca object, 
            sequencenumber integer, 
            timestamp datetime, 
            username varchar, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 