CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5WSDUP2 AS SELECT
                        src:WD2_CODE::varchar AS WD2_CODE, 
                        src:WD2_DESC::varchar AS WD2_DESC, 
                        src:WD2_LASTSAVED::datetime AS WD2_LASTSAVED, 
                        src:WD2_TIME::datetime AS WD2_TIME, 
                        src:WD2_TYPE::varchar AS WD2_TYPE, 
                        src:WD2_UPDATECOUNT::numeric(38, 10) AS WD2_UPDATECOUNT, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:WD2_LASTSAVED::datetime as ETL_LASTSAVED,
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
                                        
                src:WD2_CODE  ORDER BY 
            src:WD2_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5WSDUP2 as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:WD2_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:WD2_TIME), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:WD2_UPDATECOUNT), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:WD2_LASTSAVED), '1900-01-01')) is not null