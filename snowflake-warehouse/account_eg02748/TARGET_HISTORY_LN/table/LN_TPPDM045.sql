CREATE OR REPLACE TABLE TARGET_HISTORY_LN.LN_TPPDM045_DELETED(
            cdf_expi integer, 
            cdf_expi_kw varchar, 
            cint varchar, 
            compnr integer, 
            deleted boolean, 
            dsca object, 
            sequencenumber integer, 
            timestamp datetime, 
            username varchar, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 