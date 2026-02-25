CREATE OR REPLACE TABLE DATAHUB_TARGET.IPS_CDR_USE_USEAPPLDETAIL(
            ADDBY varchar, 
            ADDDTTM datetime, 
            APUSEAPPLDTLKEY integer, 
            APUSEAPPLDTLTYPEKEY integer, 
            APUSEKEY integer, 
            DELETED boolean, 
            MODBY varchar, 
            MODDTTM datetime, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 