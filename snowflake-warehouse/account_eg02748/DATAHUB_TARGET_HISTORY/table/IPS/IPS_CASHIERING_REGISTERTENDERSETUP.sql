CREATE OR REPLACE TABLE DATAHUB_TARGET_HISTORY.IPS_DELETED_CASHIERING_REGISTERTENDERSETUP(
            ADDBY varchar, 
            ADDDTTM datetime, 
            DATALAKE_DELETED boolean, 
            ISHARDWAREUSED varchar, 
            ISOPENSRAWERONTRANSACTION varchar, 
            ISTENDERALLOWED varchar, 
            MERCHANTACCOUNTKEY integer, 
            MODBY varchar, 
            MODDTTM datetime, 
            REGISTERTENDERSETUPKEY integer, 
            TENDERTYPE varchar, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 