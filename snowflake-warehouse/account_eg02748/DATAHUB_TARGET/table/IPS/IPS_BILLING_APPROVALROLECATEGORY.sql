CREATE OR REPLACE TABLE DATAHUB_TARGET.IPS_BILLING_APPROVALROLECATEGORY(
            ADDBY varchar, 
            ADDDTTM datetime, 
            CATEGORYKEY integer, 
            CATEGORYNAME varchar, 
            DELETED boolean, 
            MODBY varchar, 
            MODDTTM datetime, 
            PARENTCATEGORY integer, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 