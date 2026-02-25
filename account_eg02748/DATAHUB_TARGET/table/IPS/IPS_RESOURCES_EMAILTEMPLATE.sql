CREATE OR REPLACE TABLE DATAHUB_TARGET.IPS_RESOURCES_EMAILTEMPLATE(
            ADDBY varchar, 
            ADDDTTM datetime, 
            DATALAKE_DELETED boolean, 
            EMAILTEMPLATEKEY integer, 
            INPUTPARAMETERS varchar, 
            MODBY varchar, 
            MODDTTM datetime, 
            SENDASHTML varchar, 
            TEMPLATEID varchar, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 