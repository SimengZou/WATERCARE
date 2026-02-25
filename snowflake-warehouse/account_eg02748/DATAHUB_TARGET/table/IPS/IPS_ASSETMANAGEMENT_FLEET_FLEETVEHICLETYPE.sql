CREATE OR REPLACE TABLE DATAHUB_TARGET.IPS_ASSETMANAGEMENT_FLEET_FLEETVEHICLETYPE(
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
            etl_load_metadatafilename varchar
            ); 