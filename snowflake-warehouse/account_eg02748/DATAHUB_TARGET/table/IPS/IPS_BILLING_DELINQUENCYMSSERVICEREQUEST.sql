CREATE OR REPLACE TABLE DATAHUB_TARGET.IPS_BILLING_DELINQUENCYMSSERVICEREQUEST(
            ADDBY varchar, 
            ADDDTTM datetime, 
            AUTOGENERATE varchar, 
            CONDITIONFORMULA integer, 
            DELETED boolean, 
            DISPLAYORDER integer, 
            MILESTONEKEY integer, 
            MILESTONESERVICEREQUESTKEY integer, 
            MODBY varchar, 
            MODDTTM datetime, 
            PERFORMON varchar, 
            SERVICEREQUESTKEY integer, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 