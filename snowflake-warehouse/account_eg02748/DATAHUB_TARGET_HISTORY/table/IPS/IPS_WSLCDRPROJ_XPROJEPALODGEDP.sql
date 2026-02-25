CREATE OR REPLACE TABLE DATAHUB_TARGET_HISTORY.IPS_DELETED_WSLCDRPROJ_XPROJEPALODGEDP(
            ADDBY varchar, 
            ADDDTTM datetime, 
            APPROJAPPLDTLKEY integer, 
            DELETED boolean, 
            LOT varchar, 
            MODBY varchar, 
            MODDTTM datetime, 
            VARIATION_ID integer, 
            XPROJEPALODGEDPKEY integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 