CREATE OR REPLACE TABLE DATAHUB_TARGET.IPS_RESOURCES_MFG(
            ADDBY varchar, 
            ADDDTTM datetime, 
            CNTCTKEY integer, 
            CODE varchar, 
            DELETED boolean, 
            EFFDATE datetime, 
            EXPDATE datetime, 
            MFGKEY integer, 
            MODBY varchar, 
            MODDTTM datetime, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 