CREATE OR REPLACE TABLE DATAHUB_TARGET_HISTORY.IPS_DELETED_CDR_BUILDING_BLDGPLCONDLOG(
            ADDBY varchar, 
            ADDDTTM datetime, 
            APPLANPLCONDKEY integer, 
            APPLANPLCONDLOGKEY integer, 
            CHANGEBY varchar, 
            CHANGEDTTM datetime, 
            CHANGETYPE integer, 
            DELETED boolean, 
            MODBY varchar, 
            MODDTTM datetime, 
            STATUS integer, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 