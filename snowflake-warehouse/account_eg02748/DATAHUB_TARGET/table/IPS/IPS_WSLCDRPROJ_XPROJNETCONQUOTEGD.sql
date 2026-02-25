CREATE OR REPLACE TABLE DATAHUB_TARGET.IPS_WSLCDRPROJ_XPROJNETCONQUOTEGD(
            ADDBY varchar, 
            ADDDTTM datetime, 
            DELETED boolean, 
            MODBY varchar, 
            MODDTTM datetime, 
            QUANTITY numeric(38, 10), 
            QUOTEITEM varchar, 
            TOTAL numeric(38, 10), 
            UNITCHARGE numeric(38, 10), 
            VARIATION_ID integer, 
            XPROJNETCONQUOTEDPKEY integer, 
            XPROJNETCONQUOTEGDKEY integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 