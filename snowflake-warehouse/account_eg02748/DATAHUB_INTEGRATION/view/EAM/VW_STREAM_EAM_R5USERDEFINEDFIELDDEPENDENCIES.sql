CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5USERDEFINEDFIELDDEPENDENCIES AS SELECT
                        src:UFD_GRIDCACHE::varchar AS UFD_GRIDCACHE, 
                        src:UFD_LASTSAVED::datetime AS UFD_LASTSAVED, 
                        src:UFD_LAYOUTCACHE::varchar AS UFD_LAYOUTCACHE, 
                        src:UFD_PAGENAME::varchar AS UFD_PAGENAME, 
                        src:UFD_RENTITY::varchar AS UFD_RENTITY, 
                        src:UFD_UPDATECOUNT::numeric(38, 10) AS UFD_UPDATECOUNT, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:UFD_LASTSAVED::datetime as ETL_LASTSAVED,
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
                                        
                src:UFD_PAGENAME,
                src:UFD_RENTITY  ORDER BY 
            src:UFD_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5USERDEFINEDFIELDDEPENDENCIES as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:UFD_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:UFD_UPDATECOUNT), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:UFD_LASTSAVED), '1900-01-01')) is not null