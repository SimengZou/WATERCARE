CREATE OR REPLACE TABLE DATAHUB_TARGET.IPS_CORE_BATCHPROCESSING_USEREXCEPTION(
            ACCOUNTKEY integer, 
            ADDBY varchar, 
            ADDDTTM datetime, 
            BATCHPROCESSEXCEPTIONKEY integer, 
            DELETED boolean, 
            LINEITEMSETUPKEY integer, 
            MODBY varchar, 
            MODDTTM datetime, 
            RUNKEY integer, 
            USEREXCEPTIONKEY integer, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 