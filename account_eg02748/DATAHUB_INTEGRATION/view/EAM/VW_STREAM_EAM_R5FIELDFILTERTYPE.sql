CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5FIELDFILTERTYPE AS SELECT
                        src:FFT_DEFAULTSCREENTYPE::varchar AS FFT_DEFAULTSCREENTYPE, 
                        src:FFT_FUNCTION::varchar AS FFT_FUNCTION, 
                        src:FFT_LASTSAVED::datetime AS FFT_LASTSAVED, 
                        src:FFT_RTYPE::varchar AS FFT_RTYPE, 
                        src:FFT_TYPE::varchar AS FFT_TYPE, 
                        src:FFT_UPDATECOUNT::numeric(38, 10) AS FFT_UPDATECOUNT, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:FFT_LASTSAVED::datetime as ETL_LASTSAVED,
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
                                        
                src:FFT_FUNCTION,
                src:FFT_RTYPE,
                src:FFT_TYPE  ORDER BY 
            src:FFT_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5FIELDFILTERTYPE as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:FFT_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:FFT_UPDATECOUNT), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:FFT_LASTSAVED), '1900-01-01')) is not null