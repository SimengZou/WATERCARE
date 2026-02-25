CREATE OR REPLACE TABLE DATAHUB_TARGET.IPS_WSLASSETWATER_FILLINGSTATION(
            ADDBY varchar, 
            ADDDTTM datetime, 
            ADDRKEY integer, 
            CODE varchar, 
            DATALAKE_DELETED boolean, 
            DESCRIPT varchar, 
            EFFDATE datetime, 
            EXPDATE datetime, 
            MODBY varchar, 
            MODDTTM datetime, 
            POTABLE varchar, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 