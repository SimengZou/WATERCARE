CREATE OR REPLACE TABLE TARGET_HISTORY_LN.LN_TPPDM043_DELETED(
            ccco varchar, 
            ccco_ref_compnr integer, 
            compnr integer, 
            cpro varchar, 
            deleted boolean, 
            desc object, 
            seak object, 
            sequencenumber integer, 
            timestamp datetime, 
            txta integer, 
            txta_ref_compnr integer, 
            username varchar, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 