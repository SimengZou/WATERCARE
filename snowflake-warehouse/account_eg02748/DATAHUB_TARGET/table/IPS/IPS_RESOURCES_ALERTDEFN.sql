CREATE OR REPLACE TABLE DATAHUB_TARGET.IPS_RESOURCES_ALERTDEFN(
            ADDBY varchar, 
            ADDDTTM datetime, 
            ALERTDEFNKEY integer, 
            CODE varchar, 
            DELETED boolean, 
            DESCRIPTION varchar, 
            DISPFRCASH varchar, 
            EFFDATE datetime, 
            EXPDATE datetime, 
            MODBY varchar, 
            MODDTTM datetime, 
            TYPE varchar, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 