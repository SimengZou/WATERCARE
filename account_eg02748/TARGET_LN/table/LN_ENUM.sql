CREATE OR REPLACE TABLE TARGET_LN.LN_ENUM(
            const integer, 
            deleted boolean, 
            description varchar, 
            domain varchar, 
            keyword varchar, 
            language varchar, 
            sequencenumber integer, 
            timestamp datetime, 
            username varchar, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 