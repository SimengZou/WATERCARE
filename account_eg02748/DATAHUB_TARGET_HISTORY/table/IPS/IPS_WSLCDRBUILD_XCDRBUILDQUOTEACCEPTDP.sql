CREATE OR REPLACE TABLE DATAHUB_TARGET_HISTORY.IPS_DELETED_WSLCDRBUILD_XCDRBUILDQUOTEACCEPTDP(
            ADDBY varchar, 
            ADDDTTM datetime, 
            APBLDGAPPLDTLKEY integer, 
            DATALAKE_DELETED boolean, 
            IACCEPTTHEQUOTE varchar, 
            MODBY varchar, 
            MODDTTM datetime, 
            QUOTEDUEDATE datetime, 
            QUOTETOTAL varchar, 
            VARIATION_ID integer, 
            XCDRBUILDQUOTEACCEPTDPKEY integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 