CREATE OR REPLACE TABLE DATAHUB_TARGET_HISTORY.IPS_DELETED_WSLCDRBUILD_XCDRBUILDQUOTEGD(
            ADDBY varchar, 
            ADDDTTM datetime, 
            DELETED boolean, 
            MODBY varchar, 
            MODDTTM datetime, 
            QUOTEITEM varchar, 
            QUOTEQUANTITY numeric(38, 10), 
            QUOTERATE numeric(38, 10), 
            QUOTETOTAL numeric(38, 10), 
            VARIATION_ID integer, 
            XCDRBUILDQUOTEACCEPTDPKEY integer, 
            XCDRBUILDQUOTEGDKEY integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 