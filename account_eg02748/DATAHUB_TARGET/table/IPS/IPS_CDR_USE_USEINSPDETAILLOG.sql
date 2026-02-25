CREATE OR REPLACE TABLE DATAHUB_TARGET.IPS_CDR_USE_USEINSPDETAILLOG(
            ADDBY varchar, 
            ADDDTTM datetime, 
            APUSEINSPDTLKEY integer, 
            DELETED boolean, 
            DTLMODBY varchar, 
            DTLMODDTTM datetime, 
            MODBY varchar, 
            MODDTTM datetime, 
            USEINSPDETAILLOGKEY integer, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 