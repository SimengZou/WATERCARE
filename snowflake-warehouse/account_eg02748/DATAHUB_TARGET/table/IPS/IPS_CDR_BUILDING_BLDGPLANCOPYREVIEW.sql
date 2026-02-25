CREATE OR REPLACE TABLE DATAHUB_TARGET.IPS_CDR_BUILDING_BLDGPLANCOPYREVIEW(
            ADDBY varchar, 
            ADDDTTM datetime, 
            APBLDGREVIEWKEY integer, 
            BLDGPLANCOPYKEY integer, 
            BLDGPLANCOPYREVIEWKEY integer, 
            DELETED boolean, 
            MODBY varchar, 
            MODDTTM datetime, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 