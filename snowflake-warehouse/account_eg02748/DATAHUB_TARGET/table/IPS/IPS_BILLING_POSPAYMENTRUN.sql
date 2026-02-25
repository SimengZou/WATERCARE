CREATE OR REPLACE TABLE DATAHUB_TARGET.IPS_BILLING_POSPAYMENTRUN(
            ADDBY varchar, 
            ADDDTTM datetime, 
            DELETED boolean, 
            ENDRUN datetime, 
            LASTINVOCATIONSTATUS integer, 
            MODBY varchar, 
            MODDTTM datetime, 
            NUMBEROFEXCEPTIONS integer, 
            POSTPAYMENTSRUNKEY integer, 
            PROCESSINGFLAG varchar, 
            SCHEDULEKEY integer, 
            STARTRUN datetime, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 