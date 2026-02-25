CREATE OR REPLACE TABLE DATAHUB_TARGET.IPS_CDR_BUILDING_APPDETAILTYPECONSTRAINT(
            ADDBY varchar, 
            ADDDTTM datetime, 
            APPDETAILTYPECONSTRAINTKEY integer, 
            APPLICATIONDETAILTYPE integer, 
            BLDGAPPLPROCESSSTATE integer, 
            DATALAKE_DELETED boolean, 
            MODBY varchar, 
            MODDTTM datetime, 
            PORTALCUSTOMERCONSTRAINT integer, 
            PORTALPUBLICCONSTRAINT integer, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 