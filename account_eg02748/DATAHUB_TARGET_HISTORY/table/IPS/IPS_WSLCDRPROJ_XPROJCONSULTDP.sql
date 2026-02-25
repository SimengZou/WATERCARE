CREATE OR REPLACE TABLE DATAHUB_TARGET_HISTORY.IPS_DELETED_WSLCDRPROJ_XPROJCONSULTDP(
            ADDBY varchar, 
            ADDDTTM datetime, 
            APPROJAPPLDTLKEY integer, 
            ASSESSMENTTYPE varchar, 
            DELETED boolean, 
            MODBY varchar, 
            MODDTTM datetime, 
            REASONFORREQUEST varchar, 
            VARIATION_ID integer, 
            XPROJCONSULTDPKEY integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 