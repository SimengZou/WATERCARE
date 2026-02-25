CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5USERLPFTYPES AS SELECT
                        src:LPT_LASTSAVED::datetime AS LPT_LASTSAVED, 
                        src:LPT_LINEARPREFERENCE::varchar AS LPT_LINEARPREFERENCE, 
                        src:LPT_RTYPE::varchar AS LPT_RTYPE, 
                        src:LPT_SEQUENCE::numeric(38, 10) AS LPT_SEQUENCE, 
                        src:LPT_TYPE::varchar AS LPT_TYPE, 
                        src:LPT_UPDATECOUNT::numeric(38, 10) AS LPT_UPDATECOUNT, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:LPT_LASTSAVED::datetime as ETL_LASTSAVED,
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
                                        
                src:LPT_LINEARPREFERENCE,
                src:LPT_TYPE  ORDER BY 
            src:LPT_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5USERLPFTYPES as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:LPT_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:LPT_SEQUENCE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:LPT_UPDATECOUNT), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:LPT_LASTSAVED), '1900-01-01')) is not null