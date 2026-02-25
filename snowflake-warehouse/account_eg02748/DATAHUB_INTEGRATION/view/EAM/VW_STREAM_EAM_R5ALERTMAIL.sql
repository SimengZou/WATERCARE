CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5ALERTMAIL AS SELECT
                        src:ALM_ALERT::varchar AS ALM_ALERT, 
                        src:ALM_DELAY::numeric(38, 10) AS ALM_DELAY, 
                        src:ALM_DELAYUOM::varchar AS ALM_DELAYUOM, 
                        src:ALM_LASTSAVED::datetime AS ALM_LASTSAVED, 
                        src:ALM_TEMPLATE::varchar AS ALM_TEMPLATE, 
                        src:ALM_UPDATECOUNT::numeric(38, 10) AS ALM_UPDATECOUNT, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:ALM_LASTSAVED::datetime as ETL_LASTSAVED,
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
                                        
                src:ALM_ALERT,
                src:ALM_TEMPLATE  ORDER BY 
            src:ALM_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5ALERTMAIL as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ALM_DELAY), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ALM_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ALM_UPDATECOUNT), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ALM_LASTSAVED), '1900-01-01')) is not null