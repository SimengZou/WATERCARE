CREATE OR REPLACE TABLE DATAHUB_TARGET.IPS_METERMANAGEMENT_WATER_USGESTHIST(
            ADDBY varchar, 
            ADDDTTM datetime, 
            CYCLE varchar, 
            DELETED boolean, 
            MODBY varchar, 
            MODDTTM datetime, 
            NUMEST integer, 
            ROUTE integer, 
            USGESTHISTDTTM datetime, 
            USGESTHISTKEY integer, 
            USGESTRULEGRP varchar, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 