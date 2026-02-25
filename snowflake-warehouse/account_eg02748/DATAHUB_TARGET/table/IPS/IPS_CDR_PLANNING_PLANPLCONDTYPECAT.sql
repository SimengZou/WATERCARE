CREATE OR REPLACE TABLE DATAHUB_TARGET.IPS_CDR_PLANNING_PLANPLCONDTYPECAT(
            ADDBY varchar, 
            ADDDTTM datetime, 
            APPLANPLCONDTYPECATKEY integer, 
            CATNAME varchar, 
            DATALAKE_DELETED boolean, 
            MODBY varchar, 
            MODDTTM datetime, 
            PARENT integer, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 