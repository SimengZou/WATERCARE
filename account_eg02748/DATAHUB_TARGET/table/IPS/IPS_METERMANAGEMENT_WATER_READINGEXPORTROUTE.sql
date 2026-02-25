CREATE OR REPLACE TABLE DATAHUB_TARGET.IPS_METERMANAGEMENT_WATER_READINGEXPORTROUTE(
            ADDBY varchar, 
            ADDDTTM datetime, 
            DELETED boolean, 
            EXPORTOUTSTANDFLAG varchar, 
            HAVEBEENOVERWRITTEN varchar, 
            MODBY varchar, 
            MODDTTM datetime, 
            READINGEXPORTROUTEKEY integer, 
            READINGEXPORTSCHEDULEKEY integer, 
            ROUTEKEY integer, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 