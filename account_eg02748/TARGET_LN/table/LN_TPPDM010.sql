CREATE OR REPLACE TABLE TARGET_LN.LN_TPPDM010(
            compnr integer, 
            deleted boolean, 
            pmfc varchar, 
            pmfc_ref_compnr integer, 
            sequencenumber integer, 
            timestamp datetime, 
            username varchar, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 