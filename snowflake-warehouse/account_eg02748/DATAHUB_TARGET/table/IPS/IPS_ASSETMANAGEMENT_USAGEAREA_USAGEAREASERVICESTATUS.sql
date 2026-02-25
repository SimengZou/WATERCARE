CREATE OR REPLACE TABLE DATAHUB_TARGET.IPS_ASSETMANAGEMENT_USAGEAREA_USAGEAREASERVICESTATUS(
            ADDBY varchar, 
            ADDDTTM datetime, 
            CODE varchar, 
            DATALAKE_DELETED boolean, 
            DESCRIPT varchar, 
            EFFDATE datetime, 
            EXPDATE datetime, 
            INOUTSRV varchar, 
            MODBY varchar, 
            MODDTTM datetime, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 