CREATE OR REPLACE TABLE TARGET_HISTORY_LN.LN_TPPDM010_DELETED(
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
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 