CREATE OR REPLACE TABLE DATAHUB_TARGET.IPS_CDR_BUILDING_BLDGINSPDETAIL(
            ADDBY varchar, 
            ADDDTTM datetime, 
            APBLDGINSPDTLKEY integer, 
            APBLDGINSPDTLTYPEKEY integer, 
            APBLDGINSPKEY integer, 
            DATALAKE_DELETED boolean, 
            MODBY varchar, 
            MODDTTM datetime, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 