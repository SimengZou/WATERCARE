CREATE OR REPLACE TABLE DATAHUB_TARGET.IPS_BUDGETING_BGTDEFN(
            ADDBY varchar, 
            ADDDTTM datetime, 
            BGTDESC varchar, 
            BGTDFNKEY integer, 
            BGTID varchar, 
            DELETED boolean, 
            ENDDT datetime, 
            MODBY varchar, 
            MODDTTM datetime, 
            STARTDT datetime, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 