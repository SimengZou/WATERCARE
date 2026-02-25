CREATE OR REPLACE TABLE DATAHUB_TARGET_HISTORY.IPS_DELETED_WSLCDRBUILD_XBUILDFEESDP(
            ADDBY varchar, 
            ADDDTTM datetime, 
            APBLDGAPPLDTLKEY integer, 
            DELETED boolean, 
            FEETOTAL numeric(38, 10), 
            MODBY varchar, 
            MODDTTM datetime, 
            VARIATION_ID integer, 
            XBUILDFEESDPKEY integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 