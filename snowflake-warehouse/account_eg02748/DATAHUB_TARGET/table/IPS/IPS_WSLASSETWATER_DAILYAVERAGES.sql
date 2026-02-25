CREATE OR REPLACE TABLE DATAHUB_TARGET.IPS_WSLASSETWATER_DAILYAVERAGES(
            ADDBY varchar, 
            ADDDTTM datetime, 
            CODE varchar, 
            DAILYAVERAGE numeric(38, 10), 
            DELETED boolean, 
            DESCRIPT varchar, 
            EFFDATE datetime, 
            EXPDATE datetime, 
            MODBY varchar, 
            MODDTTM datetime, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 