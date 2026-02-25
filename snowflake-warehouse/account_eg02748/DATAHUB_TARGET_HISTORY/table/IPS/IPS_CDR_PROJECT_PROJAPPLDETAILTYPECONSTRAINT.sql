CREATE OR REPLACE TABLE DATAHUB_TARGET_HISTORY.IPS_DELETED_CDR_PROJECT_PROJAPPLDETAILTYPECONSTRAINT(
            ADDBY varchar, 
            ADDDTTM datetime, 
            DATALAKE_DELETED boolean, 
            MODBY varchar, 
            MODDTTM datetime, 
            PORTALCUSTOMERCONSTRAINT integer, 
            PORTALPUBLICCONSTRAINT integer, 
            PROJAPPLDETAILTYPE integer, 
            PROJAPPLDTLTYPECONSTRAINTKEY integer, 
            PROJAPPLPROCESSSTATE integer, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 