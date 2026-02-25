CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5AVAILABILITY AS SELECT
                        src:AVL_DATE::datetime AS AVL_DATE, 
                        src:AVL_HIRHOURS::numeric(38, 10) AS AVL_HIRHOURS, 
                        src:AVL_LASTSAVED::datetime AS AVL_LASTSAVED, 
                        src:AVL_MRC::varchar AS AVL_MRC, 
                        src:AVL_NETHIRED::numeric(38, 10) AS AVL_NETHIRED, 
                        src:AVL_NETHOURS::numeric(38, 10) AS AVL_NETHOURS, 
                        src:AVL_OWNHOURS::numeric(38, 10) AS AVL_OWNHOURS, 
                        src:AVL_SPECIAL::varchar AS AVL_SPECIAL, 
                        src:AVL_TRADE::varchar AS AVL_TRADE, 
                        src:AVL_UPDATECOUNT::numeric(38, 10) AS AVL_UPDATECOUNT, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:AVL_LASTSAVED::datetime as ETL_LASTSAVED,
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
                                        
                src:AVL_DATE,
                src:AVL_MRC,
                src:AVL_TRADE  ORDER BY 
            src:AVL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5AVAILABILITY as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:AVL_DATE), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:AVL_HIRHOURS), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:AVL_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:AVL_NETHIRED), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:AVL_NETHOURS), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:AVL_OWNHOURS), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:AVL_UPDATECOUNT), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:AVL_LASTSAVED), '1900-01-01')) is not null