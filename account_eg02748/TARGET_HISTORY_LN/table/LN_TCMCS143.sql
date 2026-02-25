CREATE OR REPLACE TABLE TARGET_HISTORY_LN.LN_TCMCS143_DELETED(
            abbr varchar, 
            ccty varchar, 
            ccty_ref_compnr integer, 
            compnr integer, 
            cste varchar, 
            deleted boolean, 
            dsca object, 
            sequencenumber integer, 
            timestamp datetime, 
            tzid varchar, 
            tzid_ref_compnr integer, 
            username varchar, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 