CREATE OR REPLACE TABLE DATAHUB_TARGET.IPS_PORTAL_PROFILEPERMISSION(
            ACCESSID integer, 
            ADDBY varchar, 
            ADDDTTM datetime, 
            DATALAKE_DELETED boolean, 
            MODBY varchar, 
            MODDTTM datetime, 
            PRODUCTFAMILY varchar, 
            PROFILE varchar, 
            PROFILEPERMISSIONKEY integer, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 