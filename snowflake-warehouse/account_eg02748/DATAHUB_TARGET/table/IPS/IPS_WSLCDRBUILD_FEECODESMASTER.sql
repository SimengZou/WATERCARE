CREATE OR REPLACE TABLE DATAHUB_TARGET.IPS_WSLCDRBUILD_FEECODESMASTER(
            ADDBY varchar, 
            ADDDTTM datetime, 
            BUDGETNUMBER varchar, 
            CODE varchar, 
            DELETED boolean, 
            DESCRIPT varchar, 
            EFFDATE datetime, 
            EXPDATE datetime, 
            MODBY varchar, 
            MODDTTM datetime, 
            RATE numeric(38, 10), 
            UOM varchar, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 