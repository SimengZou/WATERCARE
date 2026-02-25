CREATE OR REPLACE TABLE DATAHUB_TARGET.IPS_CDR_PROJECT_PROJPARENTCHILD(
            ADDBY varchar, 
            ADDDTTM datetime, 
            APPROJPARENTCHILDKEY integer, 
            CAPACITY varchar, 
            CHILDKEY integer, 
            DELETED boolean, 
            MODBY varchar, 
            MODDTTM datetime, 
            PARENTKEY integer, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 