CREATE OR REPLACE TABLE TARGET_HISTORY_LN.LN_TPPDM801_DELETED(
            compnr integer, 
            ctrg varchar, 
            ctrg_ref_compnr integer, 
            dcpu numeric(38, 17), 
            deleted boolean, 
            emno varchar, 
            emno_ref_compnr integer, 
            sequencenumber integer, 
            timestamp datetime, 
            username varchar, 
            usyn integer, 
            usyn_kw varchar, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 