CREATE OR REPLACE TABLE DATAHUB_TARGET.IPS_CDR_PROJECT_PROJMODELCHARACTERISTIC(
            ADDBY varchar, 
            ADDDTTM datetime, 
            DELETED boolean, 
            MODBY varchar, 
            MODDTTM datetime, 
            MODELCHARACTERISTICTYPEKEY integer, 
            PROJMODELCHARACTERISTICKEY integer, 
            PROJMODELKEY integer, 
            VALUE numeric(38, 10), 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 