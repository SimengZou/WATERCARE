CREATE OR REPLACE TABLE DATAHUB_TARGET.IPS_CDR_BUILDING_BLDGVLTN(
            ADDBY varchar, 
            ADDDTTM datetime, 
            APBLDGKEY integer, 
            APBLDGVLTNKEY integer, 
            BLDGAREA numeric(38, 10), 
            CALCVLTN numeric(38, 10), 
            DELETED boolean, 
            DESCRIPT varchar, 
            MODBY varchar, 
            MODDTTM datetime, 
            MULT numeric(38, 10), 
            SUVLTNKEY integer, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 