CREATE OR REPLACE TABLE DATAHUB_TARGET.IPS_CDR_USE_USEAPPLDETAILLOG(
            ADDBY varchar, 
            ADDDTTM datetime, 
            APUSEAPPLDTLKEY integer, 
            DELETED boolean, 
            DTLMODBY varchar, 
            DTLMODDTTM datetime, 
            MODBY varchar, 
            MODDTTM datetime, 
            USEAPPLDETAILLOGKEY integer, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 