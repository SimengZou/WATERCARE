CREATE OR REPLACE TABLE DATAHUB_TARGET.IPS_CDR_PLANNING_PLANREVIEWDETAIL(
            ADDBY varchar, 
            ADDDTTM datetime, 
            APPLANREVDTLKEY integer, 
            APPLANREVDTLTYPEKEY integer, 
            APPLANREVIEWKEY integer, 
            DATALAKE_DELETED boolean, 
            MODBY varchar, 
            MODDTTM datetime, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 