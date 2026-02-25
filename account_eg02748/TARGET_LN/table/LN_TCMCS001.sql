CREATE OR REPLACE TABLE TARGET_LN.LN_TCMCS001(
            compnr integer, 
            crnd numeric(38, 17), 
            cuni varchar, 
            deleted boolean, 
            dsca object, 
            icun varchar, 
            sequencenumber integer, 
            tccu integer, 
            tccu_kw varchar, 
            timestamp datetime, 
            uccc integer, 
            username varchar, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 