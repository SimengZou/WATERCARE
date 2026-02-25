CREATE OR REPLACE TABLE DATAHUB_TARGET.IPS_CDR_PROJECT_PROJINSPDETAILLOG(
            ADDBY varchar, 
            ADDDTTM datetime, 
            APPROJINSPDTLKEY integer, 
            DELETED boolean, 
            DTLMODBY varchar, 
            DTLMODDTTM datetime, 
            MODBY varchar, 
            MODDTTM datetime, 
            PROJINSPDETAILLOGKEY integer, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 