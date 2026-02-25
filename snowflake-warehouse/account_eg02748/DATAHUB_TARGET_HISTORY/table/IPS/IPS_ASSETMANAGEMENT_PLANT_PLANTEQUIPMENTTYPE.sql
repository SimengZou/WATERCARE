CREATE OR REPLACE TABLE DATAHUB_TARGET_HISTORY.IPS_DELETED_ASSETMANAGEMENT_PLANT_PLANTEQUIPMENTTYPE(
            ADDBY varchar, 
            ADDDTTM datetime, 
            CODE varchar, 
            DATALAKE_DELETED boolean, 
            DESCRIPT varchar, 
            EFFDATE datetime, 
            EXPDATE datetime, 
            MODBY varchar, 
            MODDTTM datetime, 
            RATE numeric(38, 10), 
            UNITOFWORK varchar, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 