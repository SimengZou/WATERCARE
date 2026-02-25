CREATE OR REPLACE TABLE DATAHUB_TARGET_HISTORY.IPS_DELETED_WSLCDRPROJ_XCDRPROJQUOTEACCEPTDP(
            ADDBY varchar, 
            ADDDTTM datetime, 
            APPROJAPPLDTLKEY integer, 
            DELETED boolean, 
            IACCEPTTHEQUOTE varchar, 
            MODBY varchar, 
            MODDTTM datetime, 
            QUOTEDUEDATE datetime, 
            QUOTETOTAL numeric(38, 10), 
            VARIATION_ID integer, 
            XCDRPROJQUOTEACCEPTDPKEY integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 