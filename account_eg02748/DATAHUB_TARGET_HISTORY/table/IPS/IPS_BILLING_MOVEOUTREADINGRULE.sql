CREATE OR REPLACE TABLE DATAHUB_TARGET_HISTORY.IPS_DELETED_BILLING_MOVEOUTREADINGRULE(
            ADDBY varchar, 
            ADDDTTM datetime, 
            CALCULATEDAYSTYPE varchar, 
            DAYSFROMMOVEOUTDATE integer, 
            DELETED boolean, 
            MODBY varchar, 
            MODDTTM datetime, 
            MOVEOUTREADINGRULEKEY integer, 
            RULEDEFN integer, 
            RULEORDER integer, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 