CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5RELIABILITYRANKINGANSWER AS SELECT
                        src:RRW_ADJUSTED::varchar AS RRW_ADJUSTED, 
                        src:RRW_CODE::varchar AS RRW_CODE, 
                        src:RRW_DESC::varchar AS RRW_DESC, 
                        src:RRW_FINDING::varchar AS RRW_FINDING, 
                        src:RRW_GOOD::varchar AS RRW_GOOD, 
                        src:RRW_LASTSAVED::datetime AS RRW_LASTSAVED, 
                        src:RRW_LEVELPK::varchar AS RRW_LEVELPK, 
                        src:RRW_NO::varchar AS RRW_NO, 
                        src:RRW_NONCONFORMITY::varchar AS RRW_NONCONFORMITY, 
                        src:RRW_OK::varchar AS RRW_OK, 
                        src:RRW_PK::varchar AS RRW_PK, 
                        src:RRW_POOR::varchar AS RRW_POOR, 
                        src:RRW_REPAIRSNEEDED::varchar AS RRW_REPAIRSNEEDED, 
                        src:RRW_RESOLUTION::varchar AS RRW_RESOLUTION, 
                        src:RRW_SEVERITY::varchar AS RRW_SEVERITY, 
                        src:RRW_UPDATECOUNT::numeric(38, 10) AS RRW_UPDATECOUNT, 
                        src:RRW_VALUE::numeric(38, 10) AS RRW_VALUE, 
                        src:RRW_YES::varchar AS RRW_YES, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:RRW_LASTSAVED::datetime as ETL_LASTSAVED,
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
                                        
                src:RRW_PK  ORDER BY 
            src:RRW_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5RELIABILITYRANKINGANSWER as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:RRW_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:RRW_UPDATECOUNT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:RRW_VALUE), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:RRW_LASTSAVED), '1900-01-01')) is not null