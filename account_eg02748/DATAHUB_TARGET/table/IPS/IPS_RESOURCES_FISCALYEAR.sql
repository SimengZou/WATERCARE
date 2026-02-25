CREATE OR REPLACE TABLE DATAHUB_TARGET.IPS_RESOURCES_FISCALYEAR(
            ADDBY varchar, 
            ADDDTTM datetime, 
            DATALAKE_DELETED boolean, 
            FISCALYEAR integer, 
            FISCALYEARKEY integer, 
            MODBY varchar, 
            MODDTTM datetime, 
            PERIODTYPE integer, 
            STARTDATE datetime, 
            VARIATION_ID integer, 
            YEARENDADJPERIODFLAG varchar, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 