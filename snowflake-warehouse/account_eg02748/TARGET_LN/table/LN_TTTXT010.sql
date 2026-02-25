CREATE OR REPLACE TABLE TARGET_LN.LN_TTTXT010(
            clan varchar, 
            compnr integer, 
            ctxt integer, 
            ctxt_clan_ref_compnr integer, 
            ctxt_ref_compnr integer, 
            deleted boolean, 
            sequencenumber integer, 
            text varchar, 
            timestamp datetime, 
            username varchar, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 