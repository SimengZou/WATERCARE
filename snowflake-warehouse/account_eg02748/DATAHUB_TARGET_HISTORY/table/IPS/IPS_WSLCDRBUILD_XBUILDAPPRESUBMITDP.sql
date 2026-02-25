CREATE OR REPLACE TABLE DATAHUB_TARGET_HISTORY.IPS_DELETED_WSLCDRBUILD_XBUILDAPPRESUBMITDP(
            ADDBY varchar, 
            ADDDTTM datetime, 
            APBLDGAPPLDTLKEY integer, 
            DATALAKE_DELETED boolean, 
            MODBY varchar, 
            MODDTTM datetime, 
            PROBLEMSRESOLVED varchar, 
            VARIATION_ID integer, 
            XBUILDAPPRESUBMITDPKEY integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 