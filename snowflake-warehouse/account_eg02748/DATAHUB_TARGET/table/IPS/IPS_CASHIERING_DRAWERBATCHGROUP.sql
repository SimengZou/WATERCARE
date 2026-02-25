CREATE OR REPLACE TABLE DATAHUB_TARGET.IPS_CASHIERING_DRAWERBATCHGROUP(
            ADDBY varchar, 
            ADDDTTM datetime, 
            CODE varchar, 
            DELETED boolean, 
            DESCRIPT varchar, 
            EFFDATE datetime, 
            EXPDATE datetime, 
            ISAUTOPOST varchar, 
            ISCOMBINEDBATCH varchar, 
            MODBY varchar, 
            MODDTTM datetime, 
            SRC varchar, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 