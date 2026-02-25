CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5RELIABILITYRANKINGQUESTS AS SELECT
                        src:RRQ_LANG::varchar AS RRQ_LANG, 
                        src:RRQ_LASTSAVED::datetime AS RRQ_LASTSAVED, 
                        src:RRQ_LEVELPK::varchar AS RRQ_LEVELPK, 
                        src:RRQ_QUESTION::varchar AS RRQ_QUESTION, 
                        src:RRQ_TRANS::varchar AS RRQ_TRANS, 
                        src:RRQ_UPDATECOUNT::numeric(38, 10) AS RRQ_UPDATECOUNT, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:RRQ_LASTSAVED::datetime as ETL_LASTSAVED,
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
                                        
                src:RRQ_LANG,
                src:RRQ_LEVELPK  ORDER BY 
            src:RRQ_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5RELIABILITYRANKINGQUESTS as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:RRQ_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:RRQ_UPDATECOUNT), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:RRQ_LASTSAVED), '1900-01-01')) is not null