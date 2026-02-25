CREATE OR REPLACE TABLE TARGET_LN.LN_TCMCS023(
            ccur varchar, 
            ccur_ref_compnr integer, 
            citg varchar, 
            compnr integer, 
            deleted boolean, 
            dsca object, 
            sequencenumber integer, 
            spco integer, 
            spco_kw varchar, 
            spec integer, 
            spec_kw varchar, 
            timestamp datetime, 
            username varchar, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 