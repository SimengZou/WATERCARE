CREATE OR REPLACE TABLE DATAHUB_TARGET.IPS_CDR_PROJECT_PROJLICREQ(
            ADDBY varchar, 
            ADDDTTM datetime, 
            APPROJKEY integer, 
            APPROJLICREQKEY integer, 
            DELETED boolean, 
            GROUPNO integer, 
            LICTYPE varchar, 
            MODBY varchar, 
            MODDTTM datetime, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 