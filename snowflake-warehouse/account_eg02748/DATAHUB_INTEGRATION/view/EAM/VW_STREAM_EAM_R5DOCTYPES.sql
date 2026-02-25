CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5DOCTYPES AS SELECT
                        src:DOT_EXT::varchar AS DOT_EXT, 
                        src:DOT_LASTSAVED::datetime AS DOT_LASTSAVED, 
                        src:DOT_TYPE::varchar AS DOT_TYPE, 
                        src:DOT_UPDATECOUNT::numeric(38, 10) AS DOT_UPDATECOUNT, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:DOT_LASTSAVED::datetime as ETL_LASTSAVED,
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
                                        
                src:DOT_EXT  ORDER BY 
            src:DOT_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5DOCTYPES as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:DOT_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DOT_UPDATECOUNT), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:DOT_LASTSAVED), '1900-01-01')) is not null