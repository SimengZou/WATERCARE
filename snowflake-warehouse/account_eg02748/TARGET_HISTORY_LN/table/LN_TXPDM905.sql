CREATE OR REPLACE TABLE TARGET_HISTORY_LN.LN_TXPDM905_DELETED(
            compnr integer, 
            deleted boolean, 
            iwic varchar, 
            iwic_ref_compnr integer, 
            prjc varchar, 
            prjc_ref_compnr integer, 
            sequencenumber integer, 
            timestamp datetime, 
            username varchar, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 