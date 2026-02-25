CREATE OR REPLACE TABLE TARGET_LN.LN_TXPDM904(
            compnr integer, 
            deleted boolean, 
            locb varchar, 
            locb_ref_compnr integer, 
            prjc varchar, 
            prjc_ref_compnr integer, 
            sequencenumber integer, 
            timestamp datetime, 
            username varchar, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 