CREATE OR REPLACE TABLE DATAHUB_TARGET.IPS_CDR_USE_USESTATUSCODE(
            ACCESSID integer, 
            ADDBY varchar, 
            ADDDTTM datetime, 
            CODE varchar, 
            DATALAKE_DELETED boolean, 
            DESCRIPT varchar, 
            EFFDATE datetime, 
            EXPDATE datetime, 
            ISPORTALCLOSEDSTATUS varchar, 
            MODBY varchar, 
            MODDTTM datetime, 
            SYSTEMLICENSE varchar, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 