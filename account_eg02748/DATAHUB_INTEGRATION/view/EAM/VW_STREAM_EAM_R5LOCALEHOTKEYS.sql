CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5LOCALEHOTKEYS AS SELECT
                        src:LHK_CODE::varchar AS LHK_CODE, 
                        src:LHK_DEFAULT::numeric(38, 10) AS LHK_DEFAULT, 
                        src:LHK_DESC::varchar AS LHK_DESC, 
                        src:LHK_EXTRA::varchar AS LHK_EXTRA, 
                        src:LHK_KEY::numeric(38, 10) AS LHK_KEY, 
                        src:LHK_LASTSAVED::datetime AS LHK_LASTSAVED, 
                        src:LHK_LOCALE::varchar AS LHK_LOCALE, 
                        src:LHK_UPDATECOUNT::numeric(38, 10) AS LHK_UPDATECOUNT, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:LHK_LASTSAVED::datetime as ETL_LASTSAVED,
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
                                        
                src:LHK_CODE,
                src:LHK_LOCALE  ORDER BY 
            src:LHK_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5LOCALEHOTKEYS as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:LHK_DEFAULT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:LHK_KEY), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:LHK_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:LHK_UPDATECOUNT), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:LHK_LASTSAVED), '1900-01-01')) is not null