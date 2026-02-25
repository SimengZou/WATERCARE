CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5WSRSPHIST AS SELECT
                        src:WSR_ADDRESS::varchar AS WSR_ADDRESS, 
                        src:WSR_DATAAREA::numeric(38, 10) AS WSR_DATAAREA, 
                        src:WSR_LASTSAVED::datetime AS WSR_LASTSAVED, 
                        src:WSR_MESSAGE::varchar AS WSR_MESSAGE, 
                        src:WSR_PROCESS::varchar AS WSR_PROCESS, 
                        src:WSR_RSTATUS::varchar AS WSR_RSTATUS, 
                        src:WSR_STATUS::varchar AS WSR_STATUS, 
                        src:WSR_STATUS_MESSAGE::varchar AS WSR_STATUS_MESSAGE, 
                        src:WSR_TIME::datetime AS WSR_TIME, 
                        src:WSR_UPDATECOUNT::numeric(38, 10) AS WSR_UPDATECOUNT, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:WSR_LASTSAVED::datetime as ETL_LASTSAVED,
            CASE
                WHEN 'EAM' = 'LN'
                THEN src:"deleted"::BOOLEAN
                WHEN 'EAM' = 'IPS'
                THEN src:"DATALAKE_DELETED"::BOOLEAN
                ELSE src:"_DELETED"::BOOLEAN
            END as ETL_DELETED, 
            etl_load_datetime,
            etl_load_metadatafilename
            FROM 
            (
            select 
                src,
                etl_load_datetime,
                etl_load_metadatafilename
                from
                (
                    SELECT
                        
                src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:WSR_MESSAGE,
                src:WSR_PROCESS  ORDER BY 
            src:WSR_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5WSRSPHIST as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:WSR_DATAAREA), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:WSR_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:WSR_TIME), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:WSR_UPDATECOUNT), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:WSR_LASTSAVED), '1900-01-01')) is not null