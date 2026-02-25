CREATE OR REPLACE TABLE TARGET_HISTORY_LN.LN_TPPDM646_DELETED(
            cins varchar, 
            cint varchar, 
            cint_cins_ref_compnr integer, 
            cint_ref_compnr integer, 
            compnr integer, 
            cprj varchar, 
            cprj_ref_compnr integer, 
            deleted boolean, 
            sequencenumber integer, 
            timestamp datetime, 
            username varchar, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 