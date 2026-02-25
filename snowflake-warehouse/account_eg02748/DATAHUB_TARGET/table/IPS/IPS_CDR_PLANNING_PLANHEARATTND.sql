CREATE OR REPLACE TABLE DATAHUB_TARGET.IPS_CDR_PLANNING_PLANHEARATTND(
            ADDBY varchar, 
            ADDDTTM datetime, 
            DELETED boolean, 
            EFFDATE datetime, 
            EXPDATE datetime, 
            MODBY varchar, 
            MODDTTM datetime, 
            NAME varchar, 
            PLANHEARKEY integer, 
            PLANHRATTKEY integer, 
            TITLE varchar, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 