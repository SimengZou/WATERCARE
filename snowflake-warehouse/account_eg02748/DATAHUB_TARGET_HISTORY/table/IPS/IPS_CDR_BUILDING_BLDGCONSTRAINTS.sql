CREATE OR REPLACE TABLE DATAHUB_TARGET_HISTORY.IPS_DELETED_CDR_BUILDING_BLDGCONSTRAINTS(
            ADDBY varchar, 
            ADDDTTM datetime, 
            APTYPEKEY integer, 
            BLDGCONSTRAINTSKEY integer, 
            CONSTRAINTCONTORLTYPE integer, 
            CONSTRAINTNAME varchar, 
            DATALAKE_DELETED boolean, 
            MILESTONE integer, 
            MODBY varchar, 
            MODDTTM datetime, 
            SETTING integer, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 