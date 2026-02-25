CREATE OR REPLACE TABLE TARGET_LN.LN_TPPDM007(
            compnr integer, 
            cwar varchar, 
            cwar_ref_compnr integer, 
            deleted boolean, 
            item varchar, 
            item_ref_compnr integer, 
            sequencenumber integer, 
            site varchar, 
            site_ref_compnr integer, 
            timestamp datetime, 
            username varchar, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 