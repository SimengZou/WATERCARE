CREATE OR REPLACE TABLE DATAHUB_TARGET.IPS_BILLING_MAPCUSTOMTABSETUP(
            ADDBY varchar, 
            ADDDTTM datetime, 
            DELETED boolean, 
            MAPCUSTOMTABSETUPKEY integer, 
            MODBY varchar, 
            MODDTTM datetime, 
            VARIATION_ID integer, 
            VISIBLELAG varchar, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 