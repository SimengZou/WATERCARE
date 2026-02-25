CREATE OR REPLACE TABLE DATAHUB_TARGET.IPS_CRM_SRINSP(
            ADDBY varchar, 
            ADDDTTM datetime, 
            COMPDTTM datetime, 
            DATALAKE_DELETED boolean, 
            INSPECTR varchar, 
            MODBY varchar, 
            MODDTTM datetime, 
            RESP varchar, 
            SERVNO integer, 
            SRINSPKEY integer, 
            STARTDTTM datetime, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 