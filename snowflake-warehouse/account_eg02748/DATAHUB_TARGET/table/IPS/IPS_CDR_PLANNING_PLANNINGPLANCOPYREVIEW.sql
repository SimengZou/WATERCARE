CREATE OR REPLACE TABLE DATAHUB_TARGET.IPS_CDR_PLANNING_PLANNINGPLANCOPYREVIEW(
            ADDBY varchar, 
            ADDDTTM datetime, 
            APPLANREVIEWKEY integer, 
            DELETED boolean, 
            MODBY varchar, 
            MODDTTM datetime, 
            PLANNINGPLANCOPYKEY integer, 
            PLANNINGPLANCOPYREVIEWKEY integer, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 