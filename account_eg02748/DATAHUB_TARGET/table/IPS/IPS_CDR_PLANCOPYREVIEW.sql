CREATE OR REPLACE TABLE DATAHUB_TARGET.IPS_CDR_PLANCOPYREVIEW(
            ADDBY varchar, 
            ADDDTTM datetime, 
            APBLDGREVIEWKEY integer, 
            APPLANREVIEWKEY integer, 
            APPROJREVIEWKEY integer, 
            APUSEREVIEWKEY integer, 
            DELETED boolean, 
            MODBY varchar, 
            MODDTTM datetime, 
            PLANCOPYKEY integer, 
            PLANCOPYREVIEWKEY integer, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 