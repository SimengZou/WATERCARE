CREATE OR REPLACE TABLE DATAHUB_TARGET.IPS_METERMANAGEMENT_WATER_READINGCYCLE(
            ADDBY varchar, 
            ADDDTTM datetime, 
            AVERAGECONSUMPTION numeric(38, 10), 
            CODE varchar, 
            DELETED boolean, 
            DESCRIPT varchar, 
            EFFDATE datetime, 
            EXPDATE datetime, 
            MODBY varchar, 
            MODDTTM datetime, 
            UNITOFMEASURE varchar, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 