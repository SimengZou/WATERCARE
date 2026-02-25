CREATE OR REPLACE TABLE DATAHUB_TARGET_HISTORY.IPS_DELETED_CASHIERING_DRAWERADJUSTMENTTYPE(
            ADDBY varchar, 
            ADDDTTM datetime, 
            ADJDES varchar, 
            BGTNO integer, 
            CODE varchar, 
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
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 