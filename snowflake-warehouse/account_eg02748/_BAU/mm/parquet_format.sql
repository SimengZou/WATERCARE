use role ROLE_DEV_DEPLOYMENT;

create OR REPLACE file format PARQUET_FORMAT_LOGICALTYPETRUE TYPE='PARQUET' BINARY_AS_TEXT = FALSE USE_LOGICAL_TYPE = TRUE;
create file format PARQUET_FORMAT_LOGICALTYPEFALSE TYPE='PARQUET' BINARY_AS_TEXT = FALSE USE_LOGICAL_TYPE = FALSE;

--


SELECT
                                $1,CURRENT_TIMESTAMP as ETL_LOAD_DATETIME,METADATA$FILENAME as ETL_LOAD_METADATAFILENAME
                                FROM @DEV_WCC_DATAWAREHOUSE.DATAHUB_INTEGRATION.STAGE_SNOWDEV(FILE_FORMAT => parquet_format);                                
                                
SELECT
                                $1
                                ,$1:m_meter_time::timestamp_ntz
                                ,$1:m_meter_time
                                FROM @DEV_WCC_DATAWAREHOUSE.DATAHUB_INTEGRATION.STAGE_SNOWDEV(FILE_FORMAT => PARQUET_FORMAT_LOGICALTYPETRUE) 
;                                

SELECT
                                $1
                                ,$1:m_meter_time::timestamp_ntz
                                ,$1:m_meter_time
                                FROM @DEV_WCC_DATAWAREHOUSE.DATAHUB_INTEGRATION.STAGE_SNOWDEV(FILE_FORMAT => PARQUET_FORMAT_LOGICALTYPEFALSE) 
;               
