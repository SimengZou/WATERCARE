CREATE OR REPLACE TABLE DATAHUB_TARGET.IPS_CDR_BUILDING_BLDGREVIEWDETAIL(
            ADDBY varchar, 
            ADDDTTM datetime, 
            APBLDGREVDTLKEY integer, 
            APBLDGREVDTLTYPEKEY integer, 
            APBLDGREVIEWKEY integer, 
            DATALAKE_DELETED boolean, 
            MODBY varchar, 
            MODDTTM datetime, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 