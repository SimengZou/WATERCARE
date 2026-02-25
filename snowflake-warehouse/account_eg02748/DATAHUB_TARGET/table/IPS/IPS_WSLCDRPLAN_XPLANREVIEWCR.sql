CREATE OR REPLACE TABLE DATAHUB_TARGET.IPS_WSLCDRPLAN_XPLANREVIEWCR(
            ADDBY varchar, 
            ADDDTTM datetime, 
            APPLANREVIEWKEY integer, 
            DELETED boolean, 
            INSTRUCTION varchar, 
            LINKTOACTION integer, 
            MODBY varchar, 
            MODDTTM datetime, 
            VARIATION_ID integer, 
            XPLANREVIEWCRKEY integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 