CREATE OR REPLACE TABLE DATAHUB_TARGET.IPS_METERMANAGEMENT_WATER_ROUTESETUP(
            ADDBY varchar, 
            ADDDTTM datetime, 
            DELETED boolean, 
            MODBY varchar, 
            MODDTTM datetime, 
            ROUTESETUPKEY integer, 
            SEQNOINTERVAL integer, 
            STARTSEQNO integer, 
            UPDSEQNOOPT varchar, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 