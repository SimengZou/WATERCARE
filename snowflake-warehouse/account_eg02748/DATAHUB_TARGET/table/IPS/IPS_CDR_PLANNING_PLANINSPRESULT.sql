CREATE OR REPLACE TABLE DATAHUB_TARGET.IPS_CDR_PLANNING_PLANINSPRESULT(
            ADDBY varchar, 
            ADDDTTM datetime, 
            AUTOCOMPLYCODEVIOLSTATUS varchar, 
            CHKFORPARTIAL varchar, 
            CODE varchar, 
            DELETED boolean, 
            DESCRIPT varchar, 
            EFFDATE datetime, 
            EXPDATE datetime, 
            MODBY varchar, 
            MODDTTM datetime, 
            RESULTACTION integer, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 