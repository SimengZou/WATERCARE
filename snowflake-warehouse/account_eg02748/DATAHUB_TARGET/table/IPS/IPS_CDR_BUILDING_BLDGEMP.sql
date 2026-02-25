CREATE OR REPLACE TABLE DATAHUB_TARGET.IPS_CDR_BUILDING_BLDGEMP(
            ADDBY varchar, 
            ADDDTTM datetime, 
            APBLDGEMPKEY integer, 
            APBLDGKEY integer, 
            CAPACITY varchar, 
            DATALAKE_DELETED boolean, 
            EMPID varchar, 
            FROMDTTM datetime, 
            MODBY varchar, 
            MODDTTM datetime, 
            TODTTM datetime, 
            TOTALHRS numeric(38, 10), 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 