CREATE OR REPLACE TABLE DATAHUB_TARGET.IPS_CDR_USE_USELICCHK(
            ADDBY varchar, 
            ADDDTTM datetime, 
            APTRADEKEY integer, 
            APUSEKEY integer, 
            APUSELICCHKKEY integer, 
            DELETED boolean, 
            MODBY varchar, 
            MODDTTM datetime, 
            REQUIREDLICENSE integer, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 