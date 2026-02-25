CREATE OR REPLACE TABLE DATAHUB_TARGET.IPS_CDR_PLANNING_PLANHEARLOG(
            ADDBY varchar, 
            ADDDTTM datetime, 
            DELETED boolean, 
            LOGBY varchar, 
            LOGDTTM datetime, 
            LOGTYPE varchar, 
            MODBY varchar, 
            MODDTTM datetime, 
            PLANHEARKEY integer, 
            PLANHRLOGKEY integer, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 