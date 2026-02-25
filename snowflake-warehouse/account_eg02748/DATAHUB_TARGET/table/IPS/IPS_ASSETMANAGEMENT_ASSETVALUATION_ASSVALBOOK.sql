CREATE OR REPLACE TABLE DATAHUB_TARGET.IPS_ASSETMANAGEMENT_ASSETVALUATION_ASSVALBOOK(
            ADDBY varchar, 
            ADDDTTM datetime, 
            CODE varchar, 
            DATALAKE_DELETED boolean, 
            DEPRECDATEOPTIONS integer, 
            DESCRIPT varchar, 
            EFFDATE datetime, 
            EXPDATE datetime, 
            MODBY varchar, 
            MODDTTM datetime, 
            PERIODSTOBACKDATE integer, 
            REVALDATEOPTIONS integer, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 